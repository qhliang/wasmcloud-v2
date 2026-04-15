use crate::bindings::custom::wechat::sender::WechatClient;
use crate::bindings::custom::wechat::types::WechatError;
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::{LOG_CTX, helpers, templates};
use serde::Deserialize;
use wstd::http::{Body, Request, Response, StatusCode};

const WECHAT_HTML: &str = include_str!("../resources/wechat.html");

pub async fn home(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    helpers::html_response(templates::render(WECHAT_HTML))
}

fn wechat_error(status: StatusCode, e: WechatError) -> anyhow::Result<Response<Body>> {
    let msg = match e {
        WechatError::Internal(s) => format!("Internal: {s}"),
        WechatError::SendFailed(s) => format!("Send failed: {s}"),
        WechatError::NotReady(s) => format!("Not ready: {s}"),
    };
    log(Level::Error, LOG_CTX, &format!("WECHAT ERROR: {}", msg));
    helpers::json_error(status, &msg)
}

// ============ Sender ============

#[derive(Deserialize)]
struct SendTextRequest {
    to: String,
    text: String,
}

pub async fn send_text(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: SendTextRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("WECHAT SEND TEXT: to={}", body.to),
    );
    let client = WechatClient::new(None);
    match client.send_text(&body.to, &body.text) {
        Ok(()) => {
            log(Level::Info, LOG_CTX, "WECHAT SEND TEXT OK");
            helpers::json_response("{\"ok\":true}")
        }
        Err(e) => wechat_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct SendMediaRequest {
    to: String,
    file_path: String,
}

pub async fn send_media(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: SendMediaRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("WECHAT SEND MEDIA: to={}", body.to),
    );
    let client = WechatClient::new(None);
    match client.send_media(&body.to, &body.file_path) {
        Ok(()) => {
            log(Level::Info, LOG_CTX, "WECHAT SEND MEDIA OK");
            helpers::json_response("{\"ok\":true}")
        }
        Err(e) => wechat_error(StatusCode::BAD_GATEWAY, e),
    }
}

// ============ QR Login ============

pub async fn qr_start(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    log(Level::Info, LOG_CTX, "WECHAT QR START");
    let client = WechatClient::new(None);
    match client.qr_start() {
        Ok(session_json) => {
            log(Level::Info, LOG_CTX, "WECHAT QR START OK");
            helpers::json_response(session_json)
        }
        Err(e) => wechat_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct QrPollRequest {
    session_json: String,
}

pub async fn qr_poll_status(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: QrPollRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "WECHAT QR POLL STATUS");
    let client = WechatClient::new(None);
    match client.qr_poll_status(&body.session_json) {
        Ok(status_json) => {
            log(Level::Info, LOG_CTX, "WECHAT QR POLL STATUS OK");
            helpers::json_response(status_json)
        }
        Err(e) => wechat_error(StatusCode::BAD_GATEWAY, e),
    }
}
