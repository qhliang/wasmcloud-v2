//! HTTP handler module for LLM Gateway API endpoints.
//!
//! Provides request routing and response formatting for OpenAI-compatible
//! `/v1/chat/completions` and Anthropic-compatible `/v1/messages` endpoints.

#![allow(dead_code)]

use genai::Client;
use genai::chat::{ChatMessage as GenaiChatMessage, ChatOptions, ChatRequest};
use tracing::debug;

use crate::LlmGatewayConfig;
use crate::anthropic_types::*;
use crate::openai_types::*;
use crate::to_genai_role;

// ── Response helpers ──────────────────────────────────────────────────────────

/// A simplified HTTP response container.
pub struct HttpResponse {
    /// HTTP status code.
    pub status: u16,
    /// Response body (JSON string or SSE event stream).
    pub body: String,
    /// Content-Type header value.
    pub content_type: &'static str,
    /// Whether this is a streaming SSE response.
    pub is_sse: bool,
}

impl HttpResponse {
    /// Build a JSON response with the given status and serializable body.
    pub fn json(status: u16, body: &impl serde::Serialize) -> Self {
        Self {
            status,
            body: serde_json::to_string(body).unwrap_or_else(|_| "{}".to_string()),
            content_type: "application/json",
            is_sse: false,
        }
    }

    /// Build an SSE response (status 200, content-type text/event-stream).
    pub fn sse(body: String) -> Self {
        Self {
            status: 200,
            body,
            content_type: "text/event-stream",
            is_sse: true,
        }
    }
}

/// Build a JSON error response in OpenAI format.
fn error_response(status: u16, code: &str, message: &str) -> HttpResponse {
    HttpResponse::json(
        status,
        &CompletionsError {
            error: CompletionsErrorDetail {
                code: Some(code.to_string()),
                message: Some(message.to_string()),
                param: None,
                r#type: Some("invalid_request_error".to_string()),
            },
        },
    )
}

// ── Main entry point ─────────────────────────────────────────────────────────

/// Route an incoming HTTP request to the appropriate handler.
///
/// Supported routes:
/// - `POST /v1/chat/completions` — OpenAI Chat Completions API
/// - `POST /v1/messages` — Anthropic Messages API
pub async fn handle_http_request(
    path: &str,
    method: &str,
    body: &str,
    config: &LlmGatewayConfig,
    client: &Client,
) -> HttpResponse {
    if method != "POST" {
        return error_response(405, "method_not_allowed", "only POST is supported");
    }

    match path {
        "/v1/chat/completions" => handle_completions(body, config, client).await,
        "/v1/messages" => handle_responses(body, config, client).await,
        _ => error_response(404, "not_found", "unknown endpoint"),
    }
}

// ── OpenAI completions handlers ──────────────────────────────────────────────

/// Handle `/v1/chat/completions` requests (sync or streaming).
async fn handle_completions(
    body: &str,
    config: &LlmGatewayConfig,
    client: &Client,
) -> HttpResponse {
    let req = match serde_json::from_str::<CompletionsRequest>(body) {
        Ok(r) => r,
        Err(e) => {
            return error_response(
                400,
                "invalid_request",
                &format!("failed to parse request: {e}"),
            );
        }
    };

    if req.messages.is_empty() {
        return error_response(400, "invalid_request", "messages must not be empty");
    }

    let is_stream = req.stream.unwrap_or(false);

    if is_stream {
        handle_completions_streaming(&req, config, client).await
    } else {
        handle_completions_sync(&req, config, client).await
    }
}

/// Handle a synchronous (non-streaming) OpenAI completions request.
async fn handle_completions_sync(
    req: &CompletionsRequest,
    config: &LlmGatewayConfig,
    client: &Client,
) -> HttpResponse {
    let messages = build_genai_messages_from_completions(req, config);
    let chat_req = ChatRequest::new(messages);
    let chat_options = build_chat_options_from_completions(config, req);

    let chat_res = match client
        .exec_chat(&req.model, chat_req, Some(&chat_options))
        .await
    {
        Ok(res) => res,
        Err(e) => {
            debug!(error = %e, "OpenAI completions request failed");
            return error_response(502, "provider_error", &format!("LLM provider error: {e}"));
        }
    };

    let content = chat_res.first_text().unwrap_or("").to_string();
    let model_name = chat_res.model_iden.model_name.to_string();
    let finish_reason = chat_res
        .stop_reason
        .as_ref()
        .map(|r| format!("{r}"))
        .unwrap_or_else(|| "stop".to_string());

    let prompt_tokens = chat_res.usage.prompt_tokens.unwrap_or(0) as u64;
    let completion_tokens = chat_res.usage.completion_tokens.unwrap_or(0) as u64;

    let response = CompletionsResponse {
        id: format!("chatcmpl-{}", uuid::Uuid::new_v4()),
        object: "chat.completion",
        model: model_name,
        choices: vec![CompletionsChoice {
            index: 0,
            message: CompletionsResponseMessage {
                role: "assistant".to_string(),
                content: Some(content),
            },
            finish_reason,
        }],
        usage: CompletionsUsage {
            prompt_tokens,
            completion_tokens,
            total_tokens: prompt_tokens + completion_tokens,
        },
    };

    HttpResponse::json(200, &response)
}

/// Handle a streaming OpenAI completions request using SSE.
async fn handle_completions_streaming(
    req: &CompletionsRequest,
    config: &LlmGatewayConfig,
    client: &Client,
) -> HttpResponse {
    let messages = build_genai_messages_from_completions(req, config);
    let chat_req = ChatRequest::new(messages);
    let chat_options = build_chat_options_from_completions(config, req)
        .with_capture_usage(true)
        .with_capture_content(true);

    let stream_res = match client
        .exec_chat_stream(&req.model, chat_req, Some(&chat_options))
        .await
    {
        Ok(res) => res,
        Err(e) => {
            debug!(error = %e, "OpenAI streaming completions request failed");
            return error_response(502, "provider_error", &format!("LLM provider error: {e}"));
        }
    };

    let completion_id = format!("chatcmpl-{}", uuid::Uuid::new_v4());
    let model_name = stream_res.model_iden.model_name.to_string();

    let mut sse_output = String::new();

    // First chunk with role
    let first_chunk = CompletionsChunk {
        id: completion_id.clone(),
        object: "chat.completion.chunk",
        model: model_name.clone(),
        choices: vec![CompletionsChunkChoice {
            index: 0,
            delta: CompletionsChunkDelta {
                role: Some("assistant".to_string()),
                content: None,
            },
            finish_reason: None,
        }],
    };
    sse_output.push_str(&format_sse_event(&first_chunk));

    // Process stream events
    let mut stream = stream_res.stream;
    use futures::StreamExt;

    while let Some(event_result) = stream.next().await {
        match event_result {
            Ok(event) => match event {
                genai::chat::ChatStreamEvent::Start => {}
                genai::chat::ChatStreamEvent::Chunk(chunk) => {
                    let chunk_event = CompletionsChunk {
                        id: completion_id.clone(),
                        object: "chat.completion.chunk",
                        model: model_name.clone(),
                        choices: vec![CompletionsChunkChoice {
                            index: 0,
                            delta: CompletionsChunkDelta {
                                role: None,
                                content: Some(chunk.content),
                            },
                            finish_reason: None,
                        }],
                    };
                    sse_output.push_str(&format_sse_event(&chunk_event));
                }
                genai::chat::ChatStreamEvent::End(stream_end) => {
                    let finish_reason = stream_end
                        .captured_stop_reason
                        .as_ref()
                        .map(|r| format!("{r}"))
                        .unwrap_or_else(|| "stop".to_string());

                    let final_chunk = CompletionsChunk {
                        id: completion_id.clone(),
                        object: "chat.completion.chunk",
                        model: model_name.clone(),
                        choices: vec![CompletionsChunkChoice {
                            index: 0,
                            delta: CompletionsChunkDelta {
                                role: None,
                                content: None,
                            },
                            finish_reason: Some(finish_reason),
                        }],
                    };
                    sse_output.push_str(&format_sse_event(&final_chunk));
                }
                _ => {}
            },
            Err(e) => {
                debug!(error = %e, "Stream error during completions streaming");
                break;
            }
        }
    }

    // SSE stream terminator
    sse_output.push_str("data: [DONE]\n\n");

    HttpResponse::sse(sse_output)
}

// ── Anthropic responses handlers ─────────────────────────────────────────────

/// Handle `/v1/messages` requests (sync or streaming).
async fn handle_responses(body: &str, config: &LlmGatewayConfig, client: &Client) -> HttpResponse {
    let req = match serde_json::from_str::<ResponsesRequest>(body) {
        Ok(r) => r,
        Err(e) => {
            let err_resp = ResponsesError {
                error: ResponsesErrorDetail {
                    error_type: "invalid_request_error".to_string(),
                    message: Some(format!("failed to parse request: {e}")),
                },
            };
            return HttpResponse::json(400, &err_resp);
        }
    };

    if req.messages.is_empty() {
        let err_resp = ResponsesError {
            error: ResponsesErrorDetail {
                error_type: "invalid_request_error".to_string(),
                message: Some("messages must not be empty".to_string()),
            },
        };
        return HttpResponse::json(400, &err_resp);
    }

    let is_stream = req.stream.unwrap_or(false);

    if is_stream {
        handle_responses_streaming(&req, config, client).await
    } else {
        handle_responses_sync(&req, config, client).await
    }
}

/// Handle a synchronous (non-streaming) Anthropic messages request.
async fn handle_responses_sync(
    req: &ResponsesRequest,
    config: &LlmGatewayConfig,
    client: &Client,
) -> HttpResponse {
    let (system_prompt, messages) = build_genai_messages_from_responses(req, config);
    let mut chat_req = ChatRequest::new(messages);

    // Add system prompt if extracted
    if let Some(sys_msg) = system_prompt {
        chat_req = chat_req.with_system(sys_msg);
    }

    let chat_options = build_chat_options_from_responses(config, req);

    let chat_res = match client
        .exec_chat(&req.model, chat_req, Some(&chat_options))
        .await
    {
        Ok(res) => res,
        Err(e) => {
            debug!(error = %e, "Anthropic responses request failed");
            let err_resp = ResponsesError {
                error: ResponsesErrorDetail {
                    error_type: "provider_error".to_string(),
                    message: Some(format!("LLM provider error: {e}")),
                },
            };
            return HttpResponse::json(502, &err_resp);
        }
    };

    let content = chat_res.first_text().unwrap_or("").to_string();
    let model_name = chat_res.model_iden.model_name.to_string();
    let stop_reason = chat_res
        .stop_reason
        .as_ref()
        .map(anthropic_stop_reason)
        .unwrap_or_else(|| "end_turn".to_string());

    let input_tokens = chat_res.usage.prompt_tokens.unwrap_or(0) as u64;
    let output_tokens = chat_res.usage.completion_tokens.unwrap_or(0) as u64;

    let response = ResponsesResponse {
        id: format!("msg_{}", uuid::Uuid::new_v4()),
        response_type: "message".to_string(),
        role: "assistant".to_string(),
        model: model_name,
        content: vec![ResponsesContentBlock {
            block_type: "text".to_string(),
            text: Some(content),
            id: None,
            name: None,
            input: None,
        }],
        stop_reason,
        usage: ResponsesUsage {
            input_tokens,
            output_tokens,
        },
    };

    HttpResponse::json(200, &response)
}

/// Handle a streaming Anthropic messages request using SSE.
async fn handle_responses_streaming(
    req: &ResponsesRequest,
    config: &LlmGatewayConfig,
    client: &Client,
) -> HttpResponse {
    let (system_prompt, messages) = build_genai_messages_from_responses(req, config);
    let mut chat_req = ChatRequest::new(messages);

    if let Some(sys_msg) = system_prompt {
        chat_req = chat_req.with_system(sys_msg);
    }

    let chat_options = build_chat_options_from_responses(config, req)
        .with_capture_usage(true)
        .with_capture_content(true);

    let stream_res = match client
        .exec_chat_stream(&req.model, chat_req, Some(&chat_options))
        .await
    {
        Ok(res) => res,
        Err(e) => {
            debug!(error = %e, "Anthropic streaming responses request failed");
            let err_resp = ResponsesError {
                error: ResponsesErrorDetail {
                    error_type: "provider_error".to_string(),
                    message: Some(format!("LLM provider error: {e}")),
                },
            };
            return HttpResponse::json(502, &err_resp);
        }
    };

    let msg_id = format!("msg_{}", uuid::Uuid::new_v4());
    let model_name = stream_res.model_iden.model_name.to_string();

    let mut sse_output = String::new();

    // message_start event
    let start_event = MessageStartEvent {
        event_type: "message_start".to_string(),
        message: ResponsesResponse {
            id: msg_id.clone(),
            response_type: "message".to_string(),
            role: "assistant".to_string(),
            model: model_name.clone(),
            content: vec![],
            stop_reason: String::new(),
            usage: ResponsesUsage {
                input_tokens: 0,
                output_tokens: 0,
            },
        },
    };
    sse_output.push_str(&format_sse_event_with_type("message_start", &start_event));

    // content_block_start event
    let block_start = ContentBlockStartEvent {
        event_type: "content_block_start".to_string(),
        index: 0,
        content_block: ResponsesContentBlock {
            block_type: "text".to_string(),
            text: Some(String::new()),
            id: None,
            name: None,
            input: None,
        },
    };
    sse_output.push_str(&format_sse_event_with_type(
        "content_block_start",
        &block_start,
    ));

    // Process stream events
    let mut stream = stream_res.stream;
    use futures::StreamExt;

    while let Some(event_result) = stream.next().await {
        match event_result {
            Ok(event) => match event {
                genai::chat::ChatStreamEvent::Start => {}
                genai::chat::ChatStreamEvent::Chunk(chunk) => {
                    let delta_event = ContentBlockDeltaEvent {
                        event_type: "content_block_delta".to_string(),
                        index: 0,
                        delta: TextDelta {
                            delta_type: "text_delta".to_string(),
                            text: chunk.content,
                        },
                    };
                    sse_output.push_str(&format_sse_event_with_type(
                        "content_block_delta",
                        &delta_event,
                    ));
                }
                genai::chat::ChatStreamEvent::End(stream_end) => {
                    let stop_reason = stream_end
                        .captured_stop_reason
                        .as_ref()
                        .map(anthropic_stop_reason)
                        .unwrap_or_else(|| "end_turn".to_string());

                    let output_tokens = stream_end
                        .captured_usage
                        .as_ref()
                        .and_then(|u| u.completion_tokens)
                        .unwrap_or(0) as u64;
                    let input_tokens = stream_end
                        .captured_usage
                        .as_ref()
                        .and_then(|u| u.prompt_tokens)
                        .unwrap_or(0) as u64;

                    // content_block_stop event
                    let block_stop = ContentBlockStopEvent {
                        event_type: "content_block_stop".to_string(),
                        index: 0,
                    };
                    sse_output.push_str(&format_sse_event_with_type(
                        "content_block_stop",
                        &block_stop,
                    ));

                    // message_delta event
                    let msg_delta = MessageDeltaEvent {
                        event_type: "message_delta".to_string(),
                        delta: MessageDelta { stop_reason },
                        usage: ResponsesUsage {
                            input_tokens,
                            output_tokens,
                        },
                    };
                    sse_output.push_str(&format_sse_event_with_type("message_delta", &msg_delta));

                    // message_stop event
                    let msg_stop = MessageStopEvent {
                        event_type: "message_stop".to_string(),
                    };
                    sse_output.push_str(&format_sse_event_with_type("message_stop", &msg_stop));
                }
                _ => {}
            },
            Err(e) => {
                debug!(error = %e, "Stream error during responses streaming");
                break;
            }
        }
    }

    HttpResponse::sse(sse_output)
}

// ── Message building helpers ─────────────────────────────────────────────────

/// Build genai messages from an OpenAI completions request, prepending system prompts.
fn build_genai_messages_from_completions(
    req: &CompletionsRequest,
    config: &LlmGatewayConfig,
) -> Vec<GenaiChatMessage> {
    let mut messages = Vec::new();

    // Add preset system prompts from config
    for prompt in &config.system_prompts {
        match to_genai_role(&prompt.role) {
            genai::chat::ChatRole::System => {
                messages.push(GenaiChatMessage::system(&prompt.content));
            }
            genai::chat::ChatRole::Assistant => {
                messages.push(GenaiChatMessage::assistant(&prompt.content));
            }
            _ => {
                messages.push(GenaiChatMessage::user(&prompt.content));
            }
        }
    }

    // Add request messages
    for msg in &req.messages {
        match to_genai_role(&msg.role) {
            genai::chat::ChatRole::System => {
                messages.push(GenaiChatMessage::system(
                    msg.content.as_deref().unwrap_or(""),
                ));
            }
            genai::chat::ChatRole::Assistant => {
                messages.push(GenaiChatMessage::assistant(
                    msg.content.as_deref().unwrap_or(""),
                ));
            }
            _ => {
                messages.push(GenaiChatMessage::user(msg.content.as_deref().unwrap_or("")));
            }
        }
    }

    messages
}

/// Build genai messages from an Anthropic messages request.
/// Returns (optional_system_prompt, messages) tuple.
fn build_genai_messages_from_responses(
    req: &ResponsesRequest,
    config: &LlmGatewayConfig,
) -> (Option<String>, Vec<GenaiChatMessage>) {
    // Extract system prompt from Anthropic request
    let system_prompt = req.system.as_ref().and_then(|v| {
        if let Some(s) = v.as_str() {
            Some(s.to_string())
        } else if let Some(arr) = v.as_array() {
            // Array of content blocks — extract text
            let texts: Vec<String> = arr
                .iter()
                .filter_map(|block| block.get("text").and_then(|t| t.as_str()).map(String::from))
                .collect();
            if texts.is_empty() {
                None
            } else {
                Some(texts.join("\n"))
            }
        } else {
            None
        }
    });

    // Build preset prompts
    let mut preset_messages = Vec::new();
    for prompt in &config.system_prompts {
        match to_genai_role(&prompt.role) {
            genai::chat::ChatRole::System => {
                preset_messages.push(GenaiChatMessage::system(&prompt.content));
            }
            genai::chat::ChatRole::Assistant => {
                preset_messages.push(GenaiChatMessage::assistant(&prompt.content));
            }
            _ => {
                preset_messages.push(GenaiChatMessage::user(&prompt.content));
            }
        }
    }

    // Build request messages
    let mut req_messages = Vec::new();
    for msg in &req.messages {
        let content_text = extract_text_from_content(&msg.content);
        match to_genai_role(&msg.role) {
            genai::chat::ChatRole::System => {
                req_messages.push(GenaiChatMessage::system(&content_text));
            }
            genai::chat::ChatRole::Assistant => {
                req_messages.push(GenaiChatMessage::assistant(&content_text));
            }
            _ => {
                req_messages.push(GenaiChatMessage::user(&content_text));
            }
        }
    }

    let mut all_messages = preset_messages;
    all_messages.extend(req_messages);

    (system_prompt, all_messages)
}

/// Extract text from an Anthropic content value (string or array of content blocks).
fn extract_text_from_content(content: &serde_json::Value) -> String {
    if let Some(s) = content.as_str() {
        return s.to_string();
    }
    if let Some(arr) = content.as_array() {
        let texts: Vec<String> = arr
            .iter()
            .filter_map(|block| block.get("text").and_then(|t| t.as_str()).map(String::from))
            .collect();
        return texts.join("");
    }
    String::new()
}

// ── ChatOptions builders ─────────────────────────────────────────────────────

/// Build ChatOptions from config defaults + OpenAI completions request overrides.
fn build_chat_options_from_completions(
    config: &LlmGatewayConfig,
    req: &CompletionsRequest,
) -> ChatOptions {
    let mut opts = ChatOptions::default();

    // Config defaults first
    if let Some(temperature) = config.temperature {
        opts = opts.with_temperature(temperature);
    }
    if let Some(max_tokens) = config.max_tokens {
        opts = opts.with_max_tokens(max_tokens);
    }
    if let Some(top_p) = config.top_p {
        opts = opts.with_top_p(top_p);
    }

    // Request overrides
    if let Some(temperature) = req.temperature {
        opts = opts.with_temperature(temperature);
    }
    if let Some(max_tokens) = req.max_tokens {
        opts = opts.with_max_tokens(max_tokens as u32);
    }
    if let Some(top_p) = req.top_p {
        opts = opts.with_top_p(top_p);
    }

    opts
}

/// Build ChatOptions from config defaults + Anthropic responses request overrides.
fn build_chat_options_from_responses(
    config: &LlmGatewayConfig,
    req: &ResponsesRequest,
) -> ChatOptions {
    let mut opts = ChatOptions::default();

    // Config defaults first
    if let Some(temperature) = config.temperature {
        opts = opts.with_temperature(temperature);
    }
    if let Some(max_tokens) = config.max_tokens {
        opts = opts.with_max_tokens(max_tokens);
    }
    if let Some(top_p) = config.top_p {
        opts = opts.with_top_p(top_p);
    }

    // Request overrides
    if let Some(temperature) = req.temperature {
        opts = opts.with_temperature(temperature);
    }
    // Anthropic uses max_tokens as a required field
    opts = opts.with_max_tokens(req.max_tokens as u32);
    if let Some(top_p) = req.top_p {
        opts = opts.with_top_p(top_p);
    }

    opts
}

// ── SSE formatting helpers ───────────────────────────────────────────────────

/// Format a serializable value as an SSE `data:` line (no event type).
fn format_sse_event(data: &impl serde::Serialize) -> String {
    let json = serde_json::to_string(data).unwrap_or_else(|_| "{}".to_string());
    format!("data: {json}\n\n")
}

/// Format a serializable value as an SSE event with an explicit `event:` type line.
fn format_sse_event_with_type(event_type: &str, data: &impl serde::Serialize) -> String {
    let json = serde_json::to_string(data).unwrap_or_else(|_| "{}".to_string());
    format!("event: {event_type}\ndata: {json}\n\n")
}

/// Convert a genai StopReason to an Anthropic-style stop reason string.
fn anthropic_stop_reason(reason: &genai::chat::StopReason) -> String {
    match reason {
        genai::chat::StopReason::Completed(_) => "end_turn".to_string(),
        genai::chat::StopReason::MaxTokens(_) => "max_tokens".to_string(),
        genai::chat::StopReason::ToolCall(_) => "tool_use".to_string(),
        genai::chat::StopReason::StopSequence(_) => "stop_sequence".to_string(),
        genai::chat::StopReason::ContentFilter(_) => "end_turn".to_string(),
        genai::chat::StopReason::Other(s) => s.clone(),
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openai_types::CompletionsMessage;

    fn test_config() -> LlmGatewayConfig {
        LlmGatewayConfig {
            provider: genai::adapter::AdapterKind::OpenAI,
            base_url: None,
            model_name: "gpt-4o-mini".to_string(),
            api_key: "test-key".to_string(),
            temperature: Some(0.7),
            top_p: None,
            max_tokens: Some(4096),
            system_prompts: vec![crate::PresetPrompt {
                role: "system".to_string(),
                content: "You are helpful".to_string(),
            }],
        }
    }

    #[test]
    fn test_http_response_json() {
        let resp = HttpResponse::json(200, &serde_json::json!({"status": "ok"}));
        assert_eq!(resp.status, 200);
        assert_eq!(resp.content_type, "application/json");
        assert!(!resp.is_sse);
        assert!(resp.body.contains("ok"));
    }

    #[test]
    fn test_http_response_sse() {
        let resp = HttpResponse::sse("data: hello\n\n".to_string());
        assert_eq!(resp.status, 200);
        assert_eq!(resp.content_type, "text/event-stream");
        assert!(resp.is_sse);
    }

    #[test]
    fn test_error_response() {
        let resp = error_response(400, "bad_request", "invalid input");
        assert_eq!(resp.status, 400);
        assert!(resp.body.contains("bad_request"));
        assert!(resp.body.contains("invalid input"));
    }

    #[test]
    fn test_build_genai_messages_from_completions() {
        let config = test_config();
        let req = CompletionsRequest {
            model: "gpt-4o-mini".to_string(),
            messages: vec![
                CompletionsMessage {
                    role: "user".to_string(),
                    content: Some("Hello".to_string()),
                },
                CompletionsMessage {
                    role: "assistant".to_string(),
                    content: Some("Hi there".to_string()),
                },
            ],
            stream: None,
            temperature: None,
            top_p: None,
            max_tokens: None,
        };

        let messages = build_genai_messages_from_completions(&req, &config);
        // 1 preset system prompt + 2 request messages = 3
        assert_eq!(messages.len(), 3);
    }

    #[test]
    fn test_build_genai_messages_from_responses_with_system() {
        let config = test_config();
        let req = ResponsesRequest {
            model: "claude-opus-4-5".to_string(),
            messages: vec![ResponsesMessage {
                role: "user".to_string(),
                content: serde_json::json!("Hello"),
            }],
            max_tokens: 1024,
            system: Some(serde_json::json!("Be concise")),
            temperature: None,
            top_p: None,
            stream: None,
        };

        let (system_prompt, messages) = build_genai_messages_from_responses(&req, &config);
        assert_eq!(system_prompt.as_deref(), Some("Be concise"));
        // 1 preset system prompt + 1 request message = 2
        assert_eq!(messages.len(), 2);
    }

    #[test]
    fn test_build_genai_messages_from_responses_array_system() {
        let config = test_config();
        let req = ResponsesRequest {
            model: "claude-opus-4-5".to_string(),
            messages: vec![ResponsesMessage {
                role: "user".to_string(),
                content: serde_json::json!("Hello"),
            }],
            max_tokens: 512,
            system: Some(serde_json::json!([
                {"type": "text", "text": "Rule 1"},
                {"type": "text", "text": "Rule 2"}
            ])),
            temperature: None,
            top_p: None,
            stream: None,
        };

        let (system_prompt, messages) = build_genai_messages_from_responses(&req, &config);
        assert_eq!(system_prompt.as_deref(), Some("Rule 1\nRule 2"));
        assert_eq!(messages.len(), 2);
    }

    #[test]
    fn test_extract_text_from_content_string() {
        let val = serde_json::json!("Hello world");
        assert_eq!(extract_text_from_content(&val), "Hello world");
    }

    #[test]
    fn test_extract_text_from_content_array() {
        let val = serde_json::json!([
            {"type": "text", "text": "Hello "},
            {"type": "text", "text": "world"}
        ]);
        assert_eq!(extract_text_from_content(&val), "Hello world");
    }

    #[test]
    fn test_extract_text_from_content_empty() {
        let val = serde_json::json!(null);
        assert_eq!(extract_text_from_content(&val), "");
    }

    #[test]
    fn test_build_chat_options_from_completions() {
        let config = test_config();
        let req = CompletionsRequest {
            model: "gpt-4o-mini".to_string(),
            messages: vec![],
            stream: None,
            temperature: Some(0.5),
            top_p: Some(0.9),
            max_tokens: Some(2048),
        };

        let opts = build_chat_options_from_completions(&config, &req);
        assert_eq!(opts.temperature, Some(0.5));
        assert_eq!(opts.top_p, Some(0.9));
        assert_eq!(opts.max_tokens, Some(2048));
    }

    #[test]
    fn test_build_chat_options_from_responses() {
        let config = test_config();
        let req = ResponsesRequest {
            model: "claude-opus-4-5".to_string(),
            messages: vec![],
            max_tokens: 512,
            system: None,
            temperature: Some(0.3),
            top_p: None,
            stream: None,
        };

        let opts = build_chat_options_from_responses(&config, &req);
        assert_eq!(opts.temperature, Some(0.3));
        assert_eq!(opts.max_tokens, Some(512));
    }

    #[test]
    fn test_format_sse_event() {
        let data = serde_json::json!({"key": "value"});
        let output = format_sse_event(&data);
        assert!(output.starts_with("data: "));
        assert!(output.ends_with("\n\n"));
        assert!(output.contains("\"key\":\"value\""));
    }

    #[test]
    fn test_format_sse_event_with_type() {
        let data = serde_json::json!({"type": "test"});
        let output = format_sse_event_with_type("message_start", &data);
        assert!(output.starts_with("event: message_start\n"));
        assert!(output.contains("data: "));
        assert!(output.ends_with("\n\n"));
    }

    #[test]
    fn test_anthropic_stop_reason() {
        assert_eq!(
            anthropic_stop_reason(&genai::chat::StopReason::Completed("stop".to_string())),
            "end_turn"
        );
        assert_eq!(
            anthropic_stop_reason(&genai::chat::StopReason::MaxTokens("length".to_string())),
            "max_tokens"
        );
        assert_eq!(
            anthropic_stop_reason(&genai::chat::StopReason::ToolCall("tool_calls".to_string())),
            "tool_use"
        );
        assert_eq!(
            anthropic_stop_reason(&genai::chat::StopReason::StopSequence(
                "stop_sequence".to_string()
            )),
            "stop_sequence"
        );
        assert_eq!(
            anthropic_stop_reason(&genai::chat::StopReason::Other("cancelled".to_string())),
            "cancelled"
        );
    }
}
