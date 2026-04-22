mod bindings {
    use crate::Component;

    wit_bindgen::generate!({
        world: "llm-gateway-messaging",
        generate_all,
    });

    export!(Component);
}

use std::collections::BTreeMap;

use bindings::custom::llm_gateway::chat;
use bindings::custom::llm_gateway::types::{
    BinaryPart, BinarySource, ChatMessage, ChatOptions, ChatRole, ContentPart, LlmError,
    MessageContent, StopReason, ToolCall, ToolResponse,
};
use bindings::wasi::logging::logging::{Level, log};
use bindings::wasmcloud::messaging::types::BrokerMessage;

const LOG_CTX: &str = "llm-gateway-msg";

struct Component;

// --- Serde types for JSON deserialization/serialization ---

#[derive(serde::Deserialize)]
struct ChatRequest {
    model: String,
    messages: Vec<MessageInput>,
    #[serde(default)]
    options: Option<OptionsInput>,
}

#[derive(serde::Deserialize)]
struct MessageInput {
    role: String,
    content: MessageContentInput,
}

/// Accept `content` as either a plain string (backward compat) or structured parts.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum MessageContentInput {
    Parts { parts: Vec<ContentPartInput> },
    Text(String),
}

/// Individual content part in incoming JSON.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum ContentPartInput {
    Text(String),
}

#[derive(serde::Deserialize)]
struct OptionsInput {
    #[serde(default)]
    temperature: Option<f32>,
    #[serde(default)]
    max_tokens: Option<u32>,
    #[serde(default)]
    top_p: Option<f32>,
}

// --- Output types (genai-compatible NATS JSON) ---

#[derive(serde::Serialize)]
struct ChatResponseOutput {
    content: MessageContentOutput,
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    stop_reason: Option<StopReasonOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    usage: Option<TokenUsageOutput>,
}

#[derive(serde::Serialize)]
struct MessageContentOutput {
    parts: Vec<ContentPartOutput>,
}

#[derive(serde::Serialize)]
#[serde(untagged)]
enum ContentPartOutput {
    Text(String),
}

/// Serializes as a single-key map, e.g. `{"Completed": "stop"}`.
#[derive(serde::Serialize)]
struct StopReasonOutput {
    #[serde(flatten)]
    inner: BTreeMap<String, String>,
}

#[derive(serde::Serialize)]
struct TokenUsageOutput {
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
}

#[derive(serde::Serialize)]
struct ErrorReply {
    error: ErrorDetail,
}

#[derive(serde::Serialize)]
struct ErrorDetail {
    #[serde(rename = "type")]
    error_type: String,
    message: String,
}

impl bindings::exports::wasmcloud::messaging::handler::Guest for Component {
    fn handle_message(msg: BrokerMessage) -> Result<(), String> {
        let body_str = match core::str::from_utf8(&msg.body) {
            Ok(s) => s,
            Err(e) => {
                return Err(format!("invalid utf-8 in message body: {e}"));
            }
        };

        let reply_to = match msg.reply_to.as_deref() {
            Some(r) => r,
            None => {
                log(
                    Level::Warn,
                    LOG_CTX,
                    "received message with no reply-to, ignoring",
                );
                return Ok(());
            }
        };

        let request: ChatRequest = match serde_json::from_str(body_str) {
            Ok(r) => r,
            Err(e) => {
                let error_reply = ErrorReply {
                    error: ErrorDetail {
                        error_type: "invalid_request".to_string(),
                        message: format!("invalid json: {e}"),
                    },
                };
                let reply_bytes = serde_json::to_vec(&error_reply).unwrap_or_else(|_| {
                    b"{\"error\":{\"type\":\"serialization_error\",\"message\":\"failed to serialize error\"}}".to_vec()
                });
                publish_reply(reply_to, &reply_bytes);
                return Ok(());
            }
        };

        let messages: Vec<ChatMessage> = request
            .messages
            .iter()
            .map(|m| {
                let role = match m.role.as_str() {
                    "System" => ChatRole::System,
                    "Assistant" => ChatRole::Assistant,
                    "Tool" => ChatRole::Tool,
                    _ => ChatRole::User,
                };
                let parts = match &m.content {
                    MessageContentInput::Text(s) => vec![ContentPart::Text(s.clone())],
                    MessageContentInput::Parts { parts } => parts
                        .iter()
                        .map(|p| match p {
                            ContentPartInput::Text(s) => ContentPart::Text(s.clone()),
                        })
                        .collect(),
                };
                ChatMessage {
                    role,
                    content: MessageContent { parts },
                }
            })
            .collect();

        let options = request.options.map(|o| ChatOptions {
            temperature: o.temperature,
            max_tokens: o.max_tokens,
            top_p: o.top_p,
        });

        log(
            Level::Info,
            LOG_CTX,
            &format!("calling chat with model: {model}", model = request.model),
        );

        match chat::chat(&request.model, &messages, options, None) {
            Ok(response) => {
                let output = ChatResponseOutput {
                    content: MessageContentOutput {
                        parts: response
                            .content
                            .parts
                            .into_iter()
                            .map(|p| match p {
                                ContentPart::Text(s) => ContentPartOutput::Text(s),
                                ContentPart::Binary(BinaryPart {
                                    content_type,
                                    source,
                                    name: _,
                                }) => {
                                    let src = match source {
                                        BinarySource::Url(u) => u,
                                        BinarySource::Base64(b) => b,
                                    };
                                    ContentPartOutput::Text(format!(
                                        "[binary:{content_type}] {src}"
                                    ))
                                }
                                ContentPart::ToolCall(ToolCall {
                                    call_id: _,
                                    fn_name,
                                    fn_arguments,
                                }) => {
                                    ContentPartOutput::Text(format!(
                                        "[tool_call:{fn_name}] {fn_arguments}"
                                    ))
                                }
                                ContentPart::ToolResponse(ToolResponse {
                                    call_id: _,
                                    content: c,
                                }) => ContentPartOutput::Text(format!("[tool_response] {c}")),
                            })
                            .collect(),
                    },
                    model: response.model,
                    stop_reason: response.stop_reason.map(|sr| {
                        let (key, val) = match sr {
                            StopReason::Completed(s) => ("Completed", s),
                            StopReason::MaxTokens(s) => ("MaxTokens", s),
                            StopReason::ToolCall(s) => ("ToolCall", s),
                            StopReason::ContentFilter(s) => ("ContentFilter", s),
                            StopReason::StopSequence(s) => ("StopSequence", s),
                            StopReason::Other(s) => ("Other", s),
                        };
                        let mut map = BTreeMap::new();
                        map.insert(key.to_string(), val);
                        StopReasonOutput { inner: map }
                    }),
                    usage: response.usage.map(|u| TokenUsageOutput {
                        prompt_tokens: u.prompt_tokens,
                        completion_tokens: u.completion_tokens,
                        total_tokens: u.total_tokens,
                    }),
                };
                let reply_bytes = serde_json::to_vec(&output).unwrap_or_else(|_| {
                    b"{\"error\":{\"type\":\"serialization_error\",\"message\":\"failed to serialize response\"}}".to_vec()
                });
                publish_reply(reply_to, &reply_bytes);
                Ok(())
            }
            Err(e) => {
                let (error_type, message) = match e {
                    LlmError::InvalidRequest(msg) => ("invalid_request", msg),
                    LlmError::AuthenticationError(msg) => ("authentication_error", msg),
                    LlmError::ProviderError(msg) => ("provider_error", msg),
                    LlmError::RateLimitError(msg) => ("rate_limit_error", msg),
                    LlmError::ModelNotFound(msg) => ("model_not_found", msg),
                    LlmError::Unexpected(msg) => ("unexpected_error", msg),
                };
                let error_reply = ErrorReply {
                    error: ErrorDetail {
                        error_type: error_type.to_string(),
                        message,
                    },
                };
                let reply_bytes =
                    serde_json::to_vec(&error_reply).unwrap_or_else(|_| b"{\"error\":{\"type\":\"serialization_error\",\"message\":\"failed to serialize error\"}}".to_vec());
                publish_reply(reply_to, &reply_bytes);
                Ok(())
            }
        }
    }
}

fn publish_reply(reply_to: &str, body: &[u8]) {
    let msg = BrokerMessage {
        subject: reply_to.to_string(),
        body: body.to_vec(),
        reply_to: None,
    };
    if let Err(e) = bindings::wasmcloud::messaging::consumer::publish(&msg) {
        log(
            Level::Error,
            LOG_CTX,
            &format!("failed to publish reply: {e}"),
        );
    }
}
