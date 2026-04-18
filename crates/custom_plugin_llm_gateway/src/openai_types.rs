//! Serde types for the OpenAI Chat Completions API format.
//!
//! These types model the request/response shapes used by the OpenAI
//! `/v1/chat/completions` endpoint, suitable for serialization and
//! deserialization via `serde_json`.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

/// A single message in a chat completions request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsMessage {
    /// The role of the message author (e.g. "system", "user", "assistant", "tool").
    pub role: String,
    /// The text content of the message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

/// A chat completions request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsRequest {
    /// The model identifier (e.g. "gpt-4o", "gpt-4o-mini").
    pub model: String,
    /// The conversation messages.
    pub messages: Vec<CompletionsMessage>,
    /// Whether to stream partial progress via SSE.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    /// Sampling temperature (0.0 - 2.0).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    /// Nucleus sampling parameter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    /// Maximum number of tokens to generate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
}

/// The assistant message within a completions response choice.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsResponseMessage {
    /// Always "assistant".
    pub role: String,
    /// The generated text content (may be `None` if tool_calls are present).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

/// Token usage statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsUsage {
    /// Number of tokens in the prompt.
    pub prompt_tokens: u64,
    /// Number of tokens in the completion.
    pub completion_tokens: u64,
    /// Total tokens (prompt + completion).
    pub total_tokens: u64,
}

/// A single choice in a completions response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsChoice {
    /// The index of this choice.
    pub index: u32,
    /// The assistant message.
    pub message: CompletionsResponseMessage,
    /// The reason the model stopped generating (e.g. "stop", "length").
    pub finish_reason: String,
}

/// A complete chat completions response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsResponse {
    /// A unique identifier for the completion.
    pub id: String,
    /// The object type, always "chat.completion".
    pub object: &'static str,
    /// The model used for the completion.
    pub model: String,
    /// The list of completion choices.
    pub choices: Vec<CompletionsChoice>,
    /// Token usage statistics.
    pub usage: CompletionsUsage,
}

/// The delta content within a streaming chunk choice.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsChunkDelta {
    /// The role (present only in the first chunk).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// The partial text content.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

/// A single choice in a streaming chunk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsChunkChoice {
    /// The index of this choice.
    pub index: u32,
    /// The delta content for this chunk.
    pub delta: CompletionsChunkDelta,
    /// The finish reason (present only in the final chunk).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
}

/// A streaming chat completions chunk (SSE payload).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsChunk {
    /// A unique identifier for the completion.
    pub id: String,
    /// The object type, always "chat.completion.chunk".
    pub object: &'static str,
    /// The model used.
    pub model: String,
    /// The list of chunk choices.
    pub choices: Vec<CompletionsChunkChoice>,
}

/// A detail entry within an error response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsErrorDetail {
    /// The error code (e.g. "invalid_api_key").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Human-readable error message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The parameter that caused the error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub param: Option<String>,
    /// The error type (e.g. "invalid_request_error").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}

/// An OpenAI-style error response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsError {
    /// The nested error detail object.
    pub error: CompletionsErrorDetail,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_serialization() {
        let req = CompletionsRequest {
            model: "gpt-4o-mini".to_string(),
            messages: vec![CompletionsMessage {
                role: "user".to_string(),
                content: Some("Hello".to_string()),
            }],
            stream: None,
            temperature: Some(0.7),
            top_p: None,
            max_tokens: Some(1024),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"model\":\"gpt-4o-mini\""));
        assert!(json.contains("\"temperature\":0.7"));
        assert!(!json.contains("\"stream\""));
        assert!(!json.contains("\"top_p\""));
    }

    #[test]
    fn test_response_deserialization() {
        let json = r#"{
            "id": "chatcmpl-abc123",
            "object": "chat.completion",
            "model": "gpt-4o-mini",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "Hi there!"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
        }"#;
        let resp: CompletionsResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.id, "chatcmpl-abc123");
        assert_eq!(resp.object, "chat.completion");
        assert_eq!(resp.choices.len(), 1);
        assert_eq!(resp.choices[0].message.role, "assistant");
        assert_eq!(resp.usage.total_tokens, 15);
    }

    #[test]
    fn test_chunk_serialization() {
        let chunk = CompletionsChunk {
            id: "chatcmpl-xyz".to_string(),
            object: "chat.completion.chunk",
            model: "gpt-4o-mini".to_string(),
            choices: vec![CompletionsChunkChoice {
                index: 0,
                delta: CompletionsChunkDelta {
                    role: None,
                    content: Some("Hello".to_string()),
                },
                finish_reason: None,
            }],
        };
        let json = serde_json::to_string(&chunk).unwrap();
        assert!(json.contains("\"object\":\"chat.completion.chunk\""));
        assert!(!json.contains("\"finish_reason\""));
        assert!(!json.contains("\"role\""));
    }

    #[test]
    fn test_error_deserialization() {
        let json = r#"{
            "error": {
                "code": "invalid_api_key",
                "message": "Incorrect API key provided",
                "param": null,
                "type": "invalid_request_error"
            }
        }"#;
        let err: CompletionsError = serde_json::from_str(json).unwrap();
        assert_eq!(err.error.code.as_deref(), Some("invalid_api_key"));
        assert!(err.error.param.is_none());
    }

    #[test]
    fn test_request_with_stream() {
        let req = CompletionsRequest {
            model: "gpt-4o".to_string(),
            messages: vec![],
            stream: Some(true),
            temperature: None,
            top_p: None,
            max_tokens: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"stream\":true"));
    }
}
