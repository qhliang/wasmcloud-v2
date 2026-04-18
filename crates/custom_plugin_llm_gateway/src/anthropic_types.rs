//! Serde types for the Anthropic Messages API format.
//!
//! These types model the request/response shapes used by the Anthropic
//! `/v1/messages` endpoint, including SSE streaming event types.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

// ── Request types ──────────────────────────────────────────────────────────────

/// A single message in an Anthropic Messages API request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsesMessage {
    /// The role of the message author ("user" or "assistant").
    pub role: String,
    /// The message content — either a plain string or an array of content blocks.
    pub content: serde_json::Value,
}

/// An Anthropic Messages API request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsesRequest {
    /// The model identifier (e.g. "claude-opus-4-5", "claude-sonnet-4-20250514").
    pub model: String,
    /// The conversation messages.
    pub messages: Vec<ResponsesMessage>,
    /// Maximum number of tokens to generate.
    pub max_tokens: u64,
    /// System prompt (string or array of content blocks).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<serde_json::Value>,
    /// Sampling temperature (0.0 - 1.0).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    /// Nucleus sampling parameter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    /// Whether to stream via SSE.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
}

// ── Response types ─────────────────────────────────────────────────────────────

/// A content block within an Anthropic response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsesContentBlock {
    /// The block type discriminator ("text", "tool_use", etc.).
    #[serde(rename = "type")]
    pub block_type: String,
    /// The text content (present when block_type is "text").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Tool call identifier (present when block_type is "tool_use").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Tool name (present when block_type is "tool_use").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Tool input (present when block_type is "tool_use").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
}

/// Token usage statistics in an Anthropic response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsesUsage {
    /// Number of tokens in the input.
    pub input_tokens: u64,
    /// Number of tokens in the output.
    pub output_tokens: u64,
}

/// An Anthropic Messages API response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsesResponse {
    /// Unique message identifier.
    pub id: String,
    /// The response type, always "message".
    #[serde(rename = "type")]
    pub response_type: String,
    /// The role, always "assistant".
    pub role: String,
    /// The model used.
    pub model: String,
    /// The content blocks.
    pub content: Vec<ResponsesContentBlock>,
    /// The reason generation stopped (e.g. "end_turn", "max_tokens", "tool_use").
    pub stop_reason: String,
    /// Token usage statistics.
    pub usage: ResponsesUsage,
}

// ── Error types ────────────────────────────────────────────────────────────────

/// A detail entry within an Anthropic error response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsesErrorDetail {
    /// The error type (e.g. "invalid_request_error", "authentication_error").
    #[serde(rename = "type")]
    pub error_type: String,
    /// Human-readable error message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// An Anthropic-style error response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsesError {
    /// The nested error detail object.
    pub error: ResponsesErrorDetail,
}

// ── SSE streaming event types ──────────────────────────────────────────────────

/// `message_start` SSE event — emitted once at the beginning of a stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageStartEvent {
    /// Event type discriminator.
    #[serde(rename = "type")]
    pub event_type: String,
    /// The partial message object with metadata.
    pub message: ResponsesResponse,
}

/// `content_block_start` SSE event — emitted when a new content block begins.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentBlockStartEvent {
    /// Event type discriminator.
    #[serde(rename = "type")]
    pub event_type: String,
    /// The zero-based index of this content block.
    pub index: u32,
    /// The content block with initial data.
    pub content_block: ResponsesContentBlock,
}

/// A text delta within a `content_block_delta` event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextDelta {
    /// Delta type discriminator, always "text_delta".
    #[serde(rename = "type")]
    pub delta_type: String,
    /// The partial text content.
    pub text: String,
}

/// `content_block_delta` SSE event — carries incremental text updates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentBlockDeltaEvent {
    /// Event type discriminator.
    #[serde(rename = "type")]
    pub event_type: String,
    /// The zero-based index of this content block.
    pub index: u32,
    /// The text delta payload.
    pub delta: TextDelta,
}

/// `content_block_stop` SSE event — signals the end of a content block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentBlockStopEvent {
    /// Event type discriminator.
    #[serde(rename = "type")]
    pub event_type: String,
    /// The zero-based index of this content block.
    pub index: u32,
}

/// The delta payload within a `message_delta` event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageDelta {
    /// The stop reason.
    pub stop_reason: String,
}

/// `message_delta` SSE event — carries stop reason and final usage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageDeltaEvent {
    /// Event type discriminator.
    #[serde(rename = "type")]
    pub event_type: String,
    /// The delta payload with stop reason.
    pub delta: MessageDelta,
    /// Final usage statistics.
    pub usage: ResponsesUsage,
}

/// `message_stop` SSE event — signals the end of the message stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageStopEvent {
    /// Event type discriminator.
    #[serde(rename = "type")]
    pub event_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_serialization() {
        let req = ResponsesRequest {
            model: "claude-opus-4-5".to_string(),
            messages: vec![ResponsesMessage {
                role: "user".to_string(),
                content: serde_json::json!("Hello"),
            }],
            max_tokens: 1024,
            system: Some(serde_json::json!("You are a helpful assistant.")),
            temperature: Some(0.7),
            top_p: None,
            stream: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"model\":\"claude-opus-4-5\""));
        assert!(json.contains("\"max_tokens\":1024"));
        assert!(json.contains("\"system\""));
        assert!(json.contains("\"temperature\":0.7"));
        assert!(!json.contains("\"top_p\""));
        assert!(!json.contains("\"stream\""));
    }

    #[test]
    fn test_response_deserialization() {
        let json = r#"{
            "id": "msg_abc123",
            "type": "message",
            "role": "assistant",
            "model": "claude-opus-4-5",
            "content": [{"type": "text", "text": "Hello there!"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 10, "output_tokens": 5}
        }"#;
        let resp: ResponsesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.id, "msg_abc123");
        assert_eq!(resp.response_type, "message");
        assert_eq!(resp.role, "assistant");
        assert_eq!(resp.content.len(), 1);
        assert_eq!(resp.content[0].block_type, "text");
        assert_eq!(resp.content[0].text.as_deref(), Some("Hello there!"));
        assert_eq!(resp.stop_reason, "end_turn");
        assert_eq!(resp.usage.input_tokens, 10);
        assert_eq!(resp.usage.output_tokens, 5);
    }

    #[test]
    fn test_content_block_tool_use() {
        let json = r#"{
            "type": "tool_use",
            "id": "toolu_abc",
            "name": "get_weather",
            "input": {"city": "Beijing"}
        }"#;
        let block: ResponsesContentBlock = serde_json::from_str(json).unwrap();
        assert_eq!(block.block_type, "tool_use");
        assert_eq!(block.id.as_deref(), Some("toolu_abc"));
        assert_eq!(block.name.as_deref(), Some("get_weather"));
        assert_eq!(block.text, None);
    }

    #[test]
    fn test_message_start_event() {
        let json = r#"{
            "type": "message_start",
            "message": {
                "id": "msg_start",
                "type": "message",
                "role": "assistant",
                "model": "claude-opus-4-5",
                "content": [],
                "stop_reason": "",
                "usage": {"input_tokens": 10, "output_tokens": 0}
            }
        }"#;
        let event: MessageStartEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.event_type, "message_start");
        assert_eq!(event.message.id, "msg_start");
    }

    #[test]
    fn test_content_block_delta_event() {
        let json = r#"{
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "Hello"}
        }"#;
        let event: ContentBlockDeltaEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.event_type, "content_block_delta");
        assert_eq!(event.index, 0);
        assert_eq!(event.delta.delta_type, "text_delta");
        assert_eq!(event.delta.text, "Hello");
    }

    #[test]
    fn test_message_delta_event() {
        let json = r#"{
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"input_tokens": 10, "output_tokens": 42}
        }"#;
        let event: MessageDeltaEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.event_type, "message_delta");
        assert_eq!(event.delta.stop_reason, "end_turn");
        assert_eq!(event.usage.output_tokens, 42);
    }

    #[test]
    fn test_message_stop_event() {
        let json = r#"{"type": "message_stop"}"#;
        let event: MessageStopEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.event_type, "message_stop");
    }

    #[test]
    fn test_content_block_stop_event() {
        let json = r#"{"type": "content_block_stop", "index": 0}"#;
        let event: ContentBlockStopEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.event_type, "content_block_stop");
        assert_eq!(event.index, 0);
    }

    #[test]
    fn test_error_deserialization() {
        let json = r#"{
            "error": {
                "type": "authentication_error",
                "message": "invalid x-api-key"
            }
        }"#;
        let err: ResponsesError = serde_json::from_str(json).unwrap();
        assert_eq!(err.error.error_type, "authentication_error");
        assert_eq!(err.error.message.as_deref(), Some("invalid x-api-key"));
    }

    #[test]
    fn test_text_delta() {
        let json = r#"{"type": "text_delta", "text": " world"}"#;
        let delta: TextDelta = serde_json::from_str(json).unwrap();
        assert_eq!(delta.delta_type, "text_delta");
        assert_eq!(delta.text, " world");
    }
}
