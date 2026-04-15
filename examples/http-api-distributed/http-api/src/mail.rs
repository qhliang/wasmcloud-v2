use crate::bindings::custom::mail::sender::MailClient;
use crate::bindings::custom::mail::types::MailError;
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::{LOG_CTX, helpers, templates};
use serde::Deserialize;
use wstd::http::{Body, Request, Response, StatusCode};

const MAIL_HTML: &str = include_str!("../resources/mail.html");

pub async fn home(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    helpers::html_response(templates::render(MAIL_HTML))
}

#[derive(Deserialize)]
struct SendMailRequest {
    to: String,
    subject: String,
    #[serde(rename = "body_text")]
    body_text: Option<String>,
    #[serde(rename = "body_html")]
    body_html: Option<String>,
    cc: Option<String>,
    bcc: Option<String>,
}

pub async fn send_mail(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: SendMailRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, &format!("MAIL SEND: to={}", body.to));
    let client = MailClient::new(None);
    match client.send_mail(
        &body.to,
        &body.subject,
        body.body_text.as_deref(),
        body.body_html.as_deref(),
        body.cc.as_deref(),
        body.bcc.as_deref(),
    ) {
        Ok(()) => {
            log(Level::Info, LOG_CTX, "MAIL SEND OK");
            helpers::json_response("{\"ok\":true}")
        }
        Err(e) => mail_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct ListMailsRequest {
    mailbox: Option<String>,
    #[serde(rename = "search_criteria")]
    search_criteria: Option<String>,
    limit: Option<u32>,
}

pub async fn list_mails(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: ListMailsRequest = helpers::parse_json_body(&mut req).await?;
    log(Level::Info, LOG_CTX, "MAIL LIST");
    let client = MailClient::new(None);
    match client.list_mails(
        body.mailbox.as_deref(),
        body.search_criteria.as_deref(),
        body.limit,
    ) {
        Ok(json) => {
            log(Level::Info, LOG_CTX, "MAIL LIST OK");
            helpers::json_response(json)
        }
        Err(e) => mail_error(StatusCode::BAD_GATEWAY, e),
    }
}

#[derive(Deserialize)]
struct GetMailRequest {
    #[serde(rename = "message_id")]
    message_id: String,
    mailbox: Option<String>,
}

pub async fn get_mail(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: GetMailRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("MAIL GET: message_id={}", body.message_id),
    );
    let client = MailClient::new(None);
    match client.get_mail(&body.message_id, body.mailbox.as_deref()) {
        Ok(json) => {
            log(Level::Info, LOG_CTX, "MAIL GET OK");
            helpers::json_response(json)
        }
        Err(e) => mail_error(StatusCode::BAD_GATEWAY, e),
    }
}

fn mail_error(status: StatusCode, e: MailError) -> anyhow::Result<Response<Body>> {
    let msg = match e {
        MailError::ConfigError(s) => format!("Config error: {s}"),
        MailError::SendFailed(s) => format!("Send failed: {s}"),
        MailError::InvalidAddress(s) => format!("Invalid address: {s}"),
        MailError::Internal(s) => format!("Internal: {s}"),
    };
    log(Level::Error, LOG_CTX, &format!("MAIL ERROR: {}", msg));
    helpers::json_error(status, &msg)
}
