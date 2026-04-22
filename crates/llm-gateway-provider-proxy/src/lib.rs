mod bindings {
    use crate::Component;

    wit_bindgen::generate!({
        world: "llm-gateway-provider-proxy",
        generate_all,
    });

    export!(Component);
}

use std::collections::BTreeMap;

use bindings::custom::llm_gateway::types::{
    BinarySource, ChatMessage, ChatOptions, ChatResponse, ChatRole, ContentPart, LlmConfig,
    LlmError, MessageContent, StopReason, TokenUsage,
};
use bindings::exports::custom::llm_gateway::chat::Guest;
use bindings::wasi::logging::logging::{Level, log};

const LOG_CTX: &str = "llm-gateway-proxy";
const DEFAULT_SUBJECT: &str = "llm-gateway.chat";
const DEFAULT_TIMEOUT_MS: u32 = 30_000;

struct Component;

// --- Serde types matching llm-gateway-messaging JSON format ---

#[derive(serde::Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<MessageOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    options: Option<OptionsOutput>,
}

#[derive(serde::Serialize)]
struct MessageOutput {
    role: String,
    content: MessageContentOutput,
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

#[derive(serde::Serialize)]
struct OptionsOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f32>,
}

// --- Response parsing ---

#[derive(serde::Deserialize)]
struct ChatResponseInput {
    content: MessageContentInput,
    model: String,
    #[serde(default)]
    stop_reason: Option<StopReasonInput>,
    #[serde(default)]
    usage: Option<TokenUsageInput>,
}

#[derive(serde::Deserialize)]
struct MessageContentInput {
    parts: Vec<ContentPartInput>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum ContentPartInput {
    Text(String),
}

#[derive(serde::Deserialize)]
struct StopReasonInput {
    #[serde(flatten)]
    inner: BTreeMap<String, String>,
}

#[derive(serde::Deserialize)]
struct TokenUsageInput {
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
}

#[derive(serde::Deserialize)]
struct ErrorReply {
    error: ErrorDetail,
}

#[derive(serde::Deserialize)]
struct ErrorDetail {
    #[serde(rename = "type")]
    error_type: String,
    message: String,
}

impl Guest for Component {
    fn chat(
        model: String,
        messages: Vec<ChatMessage>,
        options: Option<ChatOptions>,
        config: Option<LlmConfig>,
    ) -> Result<ChatResponse, LlmError> {
        // Read config
        let subject = bindings::wasi::config::store::get("llm_request_subject")
            .ok()
            .flatten()
            .unwrap_or_else(|| DEFAULT_SUBJECT.to_string());

        let timeout_ms = bindings::wasi::config::store::get("llm_request_timeout_ms")
            .ok()
            .flatten()
            .and_then(|t| t.parse().ok())
            .unwrap_or(DEFAULT_TIMEOUT_MS);

        // Ignore dynamic config (llm-config) for proxy — provider side handles auth
        if config.is_some() {
            log(
                Level::Warn,
                LOG_CTX,
                "llm-config provided but ignored by proxy (provider side handles auth)",
            );
        }

        // Serialize request (matching llm-gateway-messaging format)
        let request = ChatRequest {
            model: model.clone(),
            messages: messages
                .iter()
                .map(|m| MessageOutput {
                    role: match m.role {
                        ChatRole::System => "System".to_string(),
                        ChatRole::User => "User".to_string(),
                        ChatRole::Assistant => "Assistant".to_string(),
                        ChatRole::Tool => "Tool".to_string(),
                    },
                    content: MessageContentOutput {
                        parts: m
                            .content
                            .parts
                            .iter()
                            .map(|p| match p {
                                ContentPart::Text(s) => ContentPartOutput::Text(s.clone()),
                                ContentPart::Binary(b) => {
                                    let src = match &b.source {
                                        BinarySource::Url(u) => u.clone(),
                                        BinarySource::Base64(b64) => b64.clone(),
                                    };
                                    ContentPartOutput::Text(format!(
                                        "[binary:{}] {}",
                                        b.content_type, src
                                    ))
                                }
                                ContentPart::ToolCall(tc) => ContentPartOutput::Text(format!(
                                    "[tool_call:{}] {}",
                                    tc.fn_name, tc.fn_arguments
                                )),
                                ContentPart::ToolResponse(tr) => ContentPartOutput::Text(format!(
                                    "[tool_response] {}",
                                    tr.content
                                )),
                            })
                            .collect(),
                    },
                })
                .collect(),
            options: options.map(|o| OptionsOutput {
                temperature: o.temperature,
                max_tokens: o.max_tokens,
                top_p: o.top_p,
            }),
        };

        let body = serde_json::to_vec(&request)
            .map_err(|e| LlmError::Unexpected(format!("failed to serialize request: {e}")))?;

        log(
            Level::Info,
            LOG_CTX,
            &format!("sending request to subject: {subject}"),
        );

        // NATS request-reply
        let reply =
            match bindings::wasmcloud::messaging::consumer::request(&subject, &body, timeout_ms) {
                Ok(msg) => msg,
                Err(e) => {
                    log(Level::Error, LOG_CTX, &format!("NATS request failed: {e}"));
                    return Err(LlmError::ProviderError(format!(
                        "messaging request failed: {e}"
                    )));
                }
            };

        log(
            Level::Debug,
            LOG_CTX,
            &format!("received reply: {} bytes", reply.body.len()),
        );

        // Parse response
        let reply_str = core::str::from_utf8(&reply.body)
            .map_err(|e| LlmError::Unexpected(format!("invalid utf-8 in reply: {e}")))?;

        // Check if it's an error response
        if let Ok(err) = serde_json::from_str::<ErrorReply>(reply_str) {
            return Err(map_error(&err.error.error_type, &err.error.message));
        }

        // Parse success response
        let response: ChatResponseInput = serde_json::from_str(reply_str)
            .map_err(|e| LlmError::Unexpected(format!("failed to parse response: {e}")))?;

        let content = MessageContent {
            parts: response
                .content
                .parts
                .into_iter()
                .map(|p| match p {
                    ContentPartInput::Text(s) => ContentPart::Text(s),
                })
                .collect(),
        };

        let stop_reason = response.stop_reason.and_then(|sr| {
            let (key, val) = sr.inner.into_iter().next()?;
            Some(match key.as_str() {
                "Completed" => StopReason::Completed(val),
                "MaxTokens" => StopReason::MaxTokens(val),
                "ToolCall" => StopReason::ToolCall(val),
                "ContentFilter" => StopReason::ContentFilter(val),
                "StopSequence" => StopReason::StopSequence(val),
                _ => StopReason::Other(val),
            })
        });

        let usage = response.usage.map(|u| TokenUsage {
            prompt_tokens: u.prompt_tokens,
            completion_tokens: u.completion_tokens,
            total_tokens: u.total_tokens,
        });

        Ok(ChatResponse {
            content,
            model: response.model,
            stop_reason,
            usage,
        })
    }
}

fn map_error(error_type: &str, message: &str) -> LlmError {
    match error_type {
        "invalid_request" => LlmError::InvalidRequest(message.to_string()),
        "authentication_error" => LlmError::AuthenticationError(message.to_string()),
        "rate_limit_error" => LlmError::RateLimitError(message.to_string()),
        "model_not_found" => LlmError::ModelNotFound(message.to_string()),
        _ => LlmError::ProviderError(message.to_string()),
    }
}
