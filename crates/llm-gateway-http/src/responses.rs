use wstd::http::{Body, Request, Response, StatusCode};

use crate::LOG_CTX;
use crate::bindings::custom::llm_gateway::chat;
use crate::bindings::custom::llm_gateway::types::{
    ChatMessage, ChatOptions, ChatRole, ContentPart, MessageContent as WitMessageContent,
    StopReason as WitStopReason,
};
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::helpers;
use crate::types::{
    ResponsesContentBlock, ResponsesError, ResponsesErrorDetail, ResponsesRequest,
    ResponsesResponse, ResponsesUsage,
};


/// Extract text content from an Anthropic-style message.
/// Anthropic `content` can be a plain string or an array of content blocks.
fn extract_text(content: &serde_json::Value) -> String {
    match content {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(blocks) => {
            let mut result = String::new();
            for block in blocks {
                if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                    if !result.is_empty() {
                        result.push('\n');
                    }
                    result.push_str(text);
                }
            }
            result
        }
        _ => String::new(),
    }
}

/// Build the list of chat messages for the LLM gateway call.
/// Handles the Anthropic `system` field by prepending it as a system message.
fn build_chat_messages(req: &ResponsesRequest) -> Vec<ChatMessage> {
    let mut messages = Vec::new();

    // Prepend system message if present
    if let Some(ref system) = req.system {
        let system_text = extract_text(system);
        if !system_text.is_empty() {
            messages.push(ChatMessage {
                role: ChatRole::System,
                content: WitMessageContent {
                    parts: vec![ContentPart::Text(system_text)],
                },
            });
        }
    }

    for msg in &req.messages {
        let text = extract_text(&msg.content);
        let role = match msg.role.as_str() {
            "system" => ChatRole::System,
            "assistant" => ChatRole::Assistant,
            "tool" => ChatRole::Tool,
            _ => ChatRole::User,
        };
        messages.push(ChatMessage {
            role,
            content: WitMessageContent {
                parts: vec![ContentPart::Text(text)],
            },
        });
    }

    messages
}

pub async fn handle(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let resp_req: ResponsesRequest = match helpers::parse_json_body(&mut req).await {
        Ok(v) => v,
        Err(e) => {
            log(
                Level::Error,
                LOG_CTX,
                &format!("failed to parse request: {e}"),
            );
            let err = ResponsesError {
                error: ResponsesErrorDetail {
                    error_type: "invalid_request_error".to_string(),
                    message: Some(format!("failed to parse request body: {e}")),
                },
            };
            return helpers::json_response(StatusCode::BAD_REQUEST, &err);
        }
    };

    if resp_req.messages.is_empty() {
        let err = ResponsesError {
            error: ResponsesErrorDetail {
                error_type: "invalid_request_error".to_string(),
                message: Some("messages must not be empty".to_string()),
            },
        };
        return helpers::json_response(StatusCode::BAD_REQUEST, &err);
    }

    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "responses: model={}, messages={}",
            resp_req.model,
            resp_req.messages.len()
        ),
    );

    let messages = build_chat_messages(&resp_req);

    let options = ChatOptions {
        temperature: resp_req.temperature.map(|t| t as f32),
        max_tokens: Some(resp_req.max_tokens as u32),
        top_p: resp_req.top_p.map(|p| p as f32),
    };

    match chat::chat(&resp_req.model, &messages, Some(options), None) {
        Ok(response) => {
            let content_text = response
                .content
                .parts
                .iter()
                .find_map(|p| match p {
                    ContentPart::Text(s) => Some(s.as_str()),
                    _ => None,
                })
                .unwrap_or("")
                .to_string();

            log(
                Level::Info,
                LOG_CTX,
                &format!(
                    "responses ok: model={}, content_len={}",
                    response.model,
                    content_text.len()
                ),
            );

            let input_tokens = response
                .usage
                .as_ref()
                .map(|u| u.prompt_tokens)
                .unwrap_or(0);
            let output_tokens = response
                .usage
                .as_ref()
                .map(|u| u.completion_tokens)
                .unwrap_or(0);

            let stop_reason = response
                .stop_reason
                .as_ref()
                .map(|sr| match sr {
                    WitStopReason::Completed(_) | WitStopReason::StopSequence(_) => {
                        "end_turn".to_string()
                    }
                    WitStopReason::MaxTokens(_) => "max_tokens".to_string(),
                    WitStopReason::ContentFilter(_) => "end_turn".to_string(),
                    WitStopReason::ToolCall(_) => "tool_use".to_string(),
                    WitStopReason::Other(_) => "end_turn".to_string(),
                })
                .unwrap_or_else(|| "end_turn".to_string());

            let result = ResponsesResponse {
                id: helpers::generate_id("msg_"),
                response_type: "message".to_string(),
                role: "assistant".to_string(),
                model: response.model,
                content: vec![ResponsesContentBlock {
                    block_type: "text".to_string(),
                    text: Some(content_text),
                }],
                stop_reason,
                usage: ResponsesUsage {
                    input_tokens,
                    output_tokens,
                },
            };

            helpers::json_response(StatusCode::OK, &result)
        }
        Err(e) => {
            log(Level::Error, LOG_CTX, &format!("responses error: {e:?}"));

            let status = helpers::map_error_status(&e);
            let err = ResponsesError {
                error: ResponsesErrorDetail {
                    error_type: helpers::map_error_type(&e),
                    message: Some(format!("{e:?}")),
                },
            };

            helpers::json_response(status, &err)
        }
    }
}
