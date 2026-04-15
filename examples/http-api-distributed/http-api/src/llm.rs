use crate::bindings::custom::llm_gateway::chat;
use crate::bindings::custom::llm_gateway::types::{ChatMessage, ChatOptions};
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::helpers;
use crate::templates;
use serde::Deserialize;
use wstd::http::{Body, Request, Response, StatusCode};

use crate::LOG_CTX;

const LLM_HTML: &str = include_str!("../resources/llm.html");

pub async fn home(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    helpers::html_response(templates::render(LLM_HTML))
}

#[derive(Deserialize)]
struct LlmChatRequest {
    model: String,
    messages: Vec<LlmMessage>,
    options: Option<LlmChatOptions>,
}

#[derive(Deserialize)]
struct LlmMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct LlmChatOptions {
    temperature: Option<f32>,
    max_tokens: Option<u32>,
    top_p: Option<f32>,
}

pub async fn chat(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let chat_req: LlmChatRequest = helpers::parse_json_body(&mut req).await?;

    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "LLM CHAT: model={}, messages={}",
            chat_req.model,
            chat_req.messages.len()
        ),
    );

    let messages: Vec<ChatMessage> = chat_req
        .messages
        .into_iter()
        .map(|m| ChatMessage {
            role: m.role,
            content: m.content,
        })
        .collect();

    let _options = chat_req.options.map(|o| ChatOptions {
        temperature: o.temperature,
        max_tokens: o.max_tokens,
        top_p: o.top_p,
    });

    match chat::chat(&chat_req.model, &messages, None, None) {
        Ok(response) => {
            log(
                Level::Info,
                LOG_CTX,
                &format!(
                    "LLM CHAT OK: model={}, content_len={}",
                    response.model,
                    response.content.len()
                ),
            );

            let json_result = serde_json::json!({
                "content": response.content,
                "model": response.model,
                "usage": response.usage.map(|u| serde_json::json!({
                    "prompt_tokens": u.prompt_tokens,
                    "completion_tokens": u.completion_tokens,
                    "total_tokens": u.total_tokens,
                })),
                "finish_reason": response.finish_reason,
            });

            helpers::json_response(serde_json::to_string(&json_result)?)
        }
        Err(e) => {
            log(Level::Error, LOG_CTX, &format!("LLM CHAT ERROR: {:?}", e));
            let error_msg = format!("{:?}", e);
            helpers::json_error(StatusCode::BAD_REQUEST, &error_msg)
        }
    }
}
