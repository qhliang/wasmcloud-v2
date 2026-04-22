mod bindings {
    use crate::Component;

    wit_bindgen::generate!({
        world: "llm-gateway-messaging",
        generate_all,
    });

    export!(Component);
}

use bindings::custom::llm_gateway::chat;
use bindings::custom::llm_gateway::types::{ChatMessage, ChatOptions, LlmError};
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
    content: String,
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

#[derive(serde::Serialize)]
struct ChatResponseOutput {
    content: String,
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    usage: Option<TokenUsageOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    finish_reason: Option<String>,
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
                let reply_bytes =
                    serde_json::to_vec(&error_reply).unwrap_or_else(|_| b"{\"error\":{\"type\":\"serialization_error\",\"message\":\"failed to serialize error\"}}".to_vec());
                publish_reply(reply_to, &reply_bytes);
                return Ok(());
            }
        };

        let messages: Vec<ChatMessage> = request
            .messages
            .iter()
            .map(|m| ChatMessage {
                role: m.role.clone(),
                content: m.content.clone(),
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
            &format!("calling chat with model: {}", request.model),
        );

        match chat::chat(&request.model, &messages, options, None) {
            Ok(response) => {
                let output = ChatResponseOutput {
                    content: response.content,
                    model: response.model,
                    usage: response.usage.map(|u| TokenUsageOutput {
                        prompt_tokens: u.prompt_tokens,
                        completion_tokens: u.completion_tokens,
                        total_tokens: u.total_tokens,
                    }),
                    finish_reason: response.finish_reason,
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
