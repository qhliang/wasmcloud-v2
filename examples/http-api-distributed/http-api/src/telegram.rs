use crate::bindings::custom::telegram::sender::TelegramBot;
use crate::bindings::custom::telegram::types::TelegramError;
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::{LOG_CTX, helpers, templates};
use serde::Deserialize;
use wstd::http::{Body, Request, Response, StatusCode};

const TELEGRAM_HTML: &str = include_str!("../resources/telegram.html");

pub async fn home(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    helpers::html_response(templates::render(TELEGRAM_HTML))
}

fn telegram_error(status: StatusCode, e: TelegramError) -> anyhow::Result<Response<Body>> {
    let msg = match e {
        TelegramError::Internal(s) => format!("Internal: {s}"),
        TelegramError::SendFailed(s) => format!("Send failed: {s}"),
        TelegramError::NotReady(s) => format!("Not ready: {s}"),
    };
    log(Level::Error, LOG_CTX, &format!("TELEGRAM ERROR: {}", msg));
    helpers::json_error(status, &msg)
}

// ============ Sender ============

#[derive(Deserialize)]
struct SendTextRequest {
    chat_id: String,
    text: String,
}

pub async fn send_text(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: SendTextRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("TELEGRAM SEND TEXT: chat_id={}", body.chat_id),
    );
    let bot = TelegramBot::new(None);
    match bot.send_text(&body.chat_id, &body.text) {
        Ok(()) => {
            log(Level::Info, LOG_CTX, "TELEGRAM SEND TEXT OK");
            helpers::json_response("{\"ok\":true}")
        }
        Err(e) => telegram_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct SendMediaRequest {
    chat_id: String,
    file_path: String,
    caption: Option<String>,
}

pub async fn send_media(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: SendMediaRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("TELEGRAM SEND MEDIA: chat_id={}", body.chat_id),
    );
    let bot = TelegramBot::new(None);
    match bot.send_media(&body.chat_id, &body.file_path, body.caption.as_deref()) {
        Ok(()) => {
            log(Level::Info, LOG_CTX, "TELEGRAM SEND MEDIA OK");
            helpers::json_response("{\"ok\":true}")
        }
        Err(e) => telegram_error(StatusCode::BAD_GATEWAY, e),
    }
}
