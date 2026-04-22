mod bindings {
    use crate::Component;

    wit_bindgen::generate!({
        world: "llm-gateway-provider-proxy",
        generate_all,
    });

    export!(Component);
}

use bindings::custom::llm_gateway::types::{
    BinarySource, ChatMessage, ChatOptions, ChatResponse, ChatRole, ContentPart, LlmConfig,
    LlmError, MessageContent, StopReason, TokenUsage,
};
use bindings::exports::custom::llm_gateway::chat::Guest;
use bindings::wasi::logging::logging::{Level, log};
use llm_gateway_types::{
    ChatOptionsJson, ChatResponseJson, ContentPartJson, ErrorReply, MessageContentJson,
};

const LOG_CTX: &str = "llm-gateway-proxy";
const DEFAULT_SUBJECT: &str = "llm-gateway.chat";
const DEFAULT_TIMEOUT_MS: u32 = 30_000;

struct Component;

// --- Request serialization type (proxy-specific) ---

#[derive(serde::Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<MessageOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    options: Option<ChatOptionsJson>,
}

#[derive(serde::Serialize)]
struct MessageOutput {
    role: String,
    content: MessageContentJson,
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
                    content: MessageContentJson {
                        parts: m
                            .content
                            .parts
                            .iter()
                            .map(|p| match p {
                                ContentPart::Text(s) => ContentPartJson::Text(s.clone()),
                                ContentPart::Binary(b) => {
                                    let src = match &b.source {
                                        BinarySource::Url(u) => u.clone(),
                                        BinarySource::Base64(b64) => b64.clone(),
                                    };
                                    ContentPartJson::Text(format!(
                                        "[binary:{}] {}",
                                        b.content_type, src
                                    ))
                                }
                                ContentPart::ToolCall(tc) => ContentPartJson::Text(format!(
                                    "[tool_call:{}] {}",
                                    tc.fn_name, tc.fn_arguments
                                )),
                                ContentPart::ToolResponse(tr) => ContentPartJson::Text(format!(
                                    "[tool_response] {}",
                                    tr.content
                                )),
                            })
                            .collect(),
                    },
                })
                .collect(),
            options: options.map(|o| ChatOptionsJson {
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
        let response: ChatResponseJson = serde_json::from_str(reply_str)
            .map_err(|e| LlmError::Unexpected(format!("failed to parse response: {e}")))?;

        let content = MessageContent {
            parts: response
                .content
                .parts
                .into_iter()
                .map(|p| match p {
                    ContentPartJson::Text(s) => ContentPart::Text(s),
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
