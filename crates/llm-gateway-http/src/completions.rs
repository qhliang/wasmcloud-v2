use std::sync::atomic::{AtomicU64, Ordering};

use wstd::http::{Body, Request, Response, StatusCode};

use crate::LOG_CTX;
use crate::bindings::custom::llm_gateway::chat;
use crate::bindings::custom::llm_gateway::types::{ChatMessage, ChatOptions, LlmError};
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::helpers;
use crate::types::{
    CompletionsChoice, CompletionsError, CompletionsErrorDetail, CompletionsMessage,
    CompletionsRequest, CompletionsResponse, CompletionsResponseMessage, CompletionsUsage,
};

static ID_COUNTER: AtomicU64 = AtomicU64::new(1);

fn generate_id() -> String {
    let id = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{id:016x}")
}

fn map_error_status(e: &LlmError) -> StatusCode {
    match e {
        LlmError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
        LlmError::AuthenticationError(_) => StatusCode::UNAUTHORIZED,
        LlmError::ProviderError(_) => StatusCode::BAD_GATEWAY,
        LlmError::RateLimitError(_) => StatusCode::TOO_MANY_REQUESTS,
        LlmError::ModelNotFound(_) => StatusCode::NOT_FOUND,
        LlmError::Unexpected(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn map_error_type(e: &LlmError) -> String {
    match e {
        LlmError::InvalidRequest(_) => "invalid_request_error",
        LlmError::AuthenticationError(_) => "authentication_error",
        LlmError::ProviderError(_) => "server_error",
        LlmError::RateLimitError(_) => "rate_limit_error",
        LlmError::ModelNotFound(_) => "invalid_request_error",
        LlmError::Unexpected(_) => "server_error",
    }
    .to_string()
}

fn map_finish_reason(reason: &str) -> String {
    match reason {
        "stop" => "stop".to_string(),
        "length" => "length".to_string(),
        "content_filter" => "content_filter".to_string(),
        other => other.to_string(),
    }
}

fn extract_content(msg: &CompletionsMessage) -> String {
    msg.content.as_deref().unwrap_or("").to_string()
}

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
        .map(|m| ChatMessage {
            role: m.role.clone(),
            content: extract_content(m),
        })
        .collect();

    let options = ChatOptions {
        temperature: chat_req.temperature.map(|t| t as f32),
        max_tokens: chat_req.max_tokens.map(|t| t as u32),
        top_p: chat_req.top_p.map(|p| p as f32),
    };

    match chat::chat(&chat_req.model, &messages, Some(options), None) {
        Ok(response) => {
            log(
                Level::Info,
                LOG_CTX,
                &format!(
                    "completions ok: model={}, content_len={}",
                    response.model,
                    response.content.len()
                ),
            );

            let usage = response.usage.as_ref();
            let prompt_tokens = usage.map(|u| u.prompt_tokens).unwrap_or(0);
            let completion_tokens = usage.map(|u| u.completion_tokens).unwrap_or(0);
            let total_tokens = usage.map(|u| u.total_tokens).unwrap_or(0);

            let finish_reason = response
                .finish_reason
                .as_deref()
                .map(map_finish_reason)
                .unwrap_or_else(|| "stop".to_string());

            let result = CompletionsResponse {
                id: format!("chatcmpl-{}", generate_id()),
                object: "chat.completion",
                model: response.model,
                choices: vec![CompletionsChoice {
                    index: 0,
                    message: CompletionsResponseMessage {
                        role: "assistant".to_string(),
                        content: Some(response.content),
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

            let status = map_error_status(&e);
            let err = CompletionsError {
                error: CompletionsErrorDetail {
                    code: None,
                    message: Some(format!("{e:?}")),
                    param: None,
                    r#type: Some(map_error_type(&e)),
                },
            };

            helpers::json_response(status, &err)
        }
    }
}
