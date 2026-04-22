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
    CompletionsChoice, CompletionsError, CompletionsErrorDetail, CompletionsRequest,
    CompletionsResponse, CompletionsResponseMessage, CompletionsUsage,
};


pub async fn handle(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let chat_req: CompletionsRequest = match helpers::parse_json_body(&mut req).await {
        Ok(v) => v,
        Err(e) => {
            log(
                Level::Error,
                LOG_CTX,
                &format!("failed to parse request: {e}"),
            );
            let err = CompletionsError {
                error: CompletionsErrorDetail {
                    code: Some("invalid_request".to_string()),
                    message: Some(format!("failed to parse request body: {e}")),
                    param: None,
                    r#type: Some("invalid_request_error".to_string()),
                },
            };
            return helpers::json_response(StatusCode::BAD_REQUEST, &err);
        }
    };

    if chat_req.messages.is_empty() {
        let err = CompletionsError {
            error: CompletionsErrorDetail {
                code: Some("invalid_request".to_string()),
                message: Some("messages must not be empty".to_string()),
                param: Some("messages".to_string()),
                r#type: Some("invalid_request_error".to_string()),
            },
        };
        return helpers::json_response(StatusCode::BAD_REQUEST, &err);
    }

    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "completions: model={}, messages={}",
            chat_req.model,
            chat_req.messages.len()
        ),
    );

    let messages: Vec<ChatMessage> = chat_req
        .messages
        .iter()
        .map(|m| {
            let role = match m.role.as_str() {
                "system" => ChatRole::System,
                "assistant" => ChatRole::Assistant,
                "tool" => ChatRole::Tool,
                _ => ChatRole::User,
            };
            ChatMessage {
                role,
                content: WitMessageContent {
                    parts: vec![ContentPart::Text(
                        m.content.clone().unwrap_or_default(),
                    )],
                },
            }
        })
        .collect();

    let options = ChatOptions {
        temperature: chat_req.temperature.map(|t| t as f32),
        max_tokens: chat_req.max_tokens.map(|t| t as u32),
        top_p: chat_req.top_p.map(|p| p as f32),
    };

    match chat::chat(&chat_req.model, &messages, Some(options), None) {
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
                    "completions ok: model={}, content_len={}",
                    response.model,
                    content_text.len()
                ),
            );

            let usage = response.usage.as_ref();
            let prompt_tokens = usage.map(|u| u.prompt_tokens).unwrap_or(0);
            let completion_tokens = usage.map(|u| u.completion_tokens).unwrap_or(0);
            let total_tokens = usage.map(|u| u.total_tokens).unwrap_or(0);

            let finish_reason = response
                .stop_reason
                .as_ref()
                .map(|sr| match sr {
                    WitStopReason::Completed(_) | WitStopReason::Other(_) => "stop".to_string(),
                    WitStopReason::MaxTokens(_) => "length".to_string(),
                    WitStopReason::ContentFilter(_) => "content_filter".to_string(),
                    WitStopReason::ToolCall(_) => "tool_calls".to_string(),
                    WitStopReason::StopSequence(_) => "stop".to_string(),
                })
                .unwrap_or_else(|| "stop".to_string());

            let result = CompletionsResponse {
                id: helpers::generate_id("chatcmpl-"),
                object: "chat.completion",
                model: response.model,
                choices: vec![CompletionsChoice {
                    index: 0,
                    message: CompletionsResponseMessage {
                        role: "assistant".to_string(),
                        content: Some(content_text),
                    },
                    finish_reason,
                }],
                usage: CompletionsUsage {
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                },
            };

            helpers::json_response(StatusCode::OK, &result)
        }
        Err(e) => {
            log(Level::Error, LOG_CTX, &format!("completions error: {e:?}"));

            let status = helpers::map_error_status(&e);
            let err = CompletionsError {
                error: CompletionsErrorDetail {
                    code: None,
                    message: Some(format!("{e:?}")),
                    param: None,
                    r#type: Some(helpers::map_error_type(&e)),
                },
            };

            helpers::json_response(status, &err)
        }
    }
}
