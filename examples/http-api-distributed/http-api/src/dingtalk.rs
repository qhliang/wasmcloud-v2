use crate::bindings::custom::dingtalk_stream::sender;
use crate::bindings::custom::dingtalk_stream::types::DingtalkError;
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::{helpers, templates, LOG_CTX};
use serde::Deserialize;
use wstd::http::{Body, Request, Response, StatusCode};

const DINGTALK_HTML: &str = include_str!("../resources/dingtalk.html");

pub async fn home(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    helpers::html_response(templates::render(DINGTALK_HTML))
}

#[derive(Deserialize)]
struct SendTextRequest {
    conversation_id: String,
    sender_id: String,
    content: String,
}

pub async fn send_text(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: SendTextRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "DINGTALK SEND TEXT: conv={}, sender={}",
            body.conversation_id, body.sender_id
        ),
    );
    match sender::send_text(&body.conversation_id, &body.sender_id, &body.content) {
        Ok(()) => {
            log(Level::Info, LOG_CTX, "DINGTALK SEND TEXT OK");
            helpers::json_response("{\"ok\":true}")
        }
        Err(e) => dingtalk_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct SendMarkdownRequest {
    conversation_id: String,
    sender_id: String,
    title: String,
    content: String,
}

pub async fn send_markdown(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: SendMarkdownRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "DINGTALK SEND MARKDOWN: conv={}, sender={}",
            body.conversation_id, body.sender_id
        ),
    );
    match sender::send_markdown(
        &body.conversation_id,
        &body.sender_id,
        &body.title,
        &body.content,
    ) {
        Ok(()) => {
            log(Level::Info, LOG_CTX, "DINGTALK SEND MARKDOWN OK");
            helpers::json_response("{\"ok\":true}")
        }
        Err(e) => dingtalk_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct SendOtoTextRequest {
    user_id: String,
    content: String,
}

pub async fn send_oto_text(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: SendOtoTextRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("DINGTALK SEND OTO TEXT: user={}", body.user_id),
    );
    match sender::send_oto_text(&body.user_id, &body.content) {
        Ok(()) => {
            log(Level::Info, LOG_CTX, "DINGTALK SEND OTO TEXT OK");
            helpers::json_response("{\"ok\":true}")
        }
        Err(e) => dingtalk_error(StatusCode::BAD_GATEWAY, e),
    }
}

pub async fn get_access_token(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    log(Level::Info, LOG_CTX, "DINGTALK GET ACCESS TOKEN");
    match sender::get_access_token() {
        Ok(token) => {
            log(Level::Info, LOG_CTX, "DINGTALK GET ACCESS TOKEN OK");
            helpers::json_response(serde_json::json!({ "token": token }).to_string())
        }
        Err(e) => dingtalk_error(StatusCode::BAD_GATEWAY, e),
    }
}

fn dingtalk_error(
    status: StatusCode,
    e: DingtalkError,
) -> anyhow::Result<Response<Body>> {
    let msg = match e {
        DingtalkError::Internal(s) => format!("Internal: {s}"),
        DingtalkError::AuthFailed(s) => format!("Auth failed: {s}"),
        DingtalkError::SendFailed(s) => format!("Send failed: {s}"),
    };
    log(Level::Error, LOG_CTX, &format!("DINGTALK ERROR: {}", msg));
    helpers::json_error(status, &msg)
}
