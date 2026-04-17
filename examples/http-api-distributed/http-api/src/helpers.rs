use std::collections::HashMap;

use anyhow::Context as _;
use serde::de::DeserializeOwned;
use wstd::http::{Body, Request, Response, StatusCode};

// ============ Response builders ============

pub fn html_response(html: impl Into<Body>) -> anyhow::Result<Response<Body>> {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/html")
        .body(html.into())
        .map_err(Into::into)
}

pub fn json_response(json: impl Into<Body>) -> anyhow::Result<Response<Body>> {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(json.into())
        .map_err(Into::into)
}

pub fn json_error(status: StatusCode, message: &str) -> anyhow::Result<Response<Body>> {
    let body = serde_json::json!({ "error": message }).to_string();
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(body.into())
        .map_err(Into::into)
}

pub fn text_response(status: StatusCode, text: impl Into<Body>) -> anyhow::Result<Response<Body>> {
    Response::builder()
        .status(status)
        .body(text.into())
        .map_err(Into::into)
}

// ============ Request parsing ============

pub fn query_params(uri: &wstd::http::Uri) -> HashMap<String, String> {
    uri.query()
        .map(|q| url::form_urlencoded::parse(q.as_bytes()).into_owned().collect())
        .unwrap_or_default()
}

pub async fn parse_json_body<T: DeserializeOwned>(req: &mut Request<Body>) -> anyhow::Result<T> {
    req.body_mut().json().await.context("failed to parse body")
}

// ============ Logging ============

pub fn log_response(result: &anyhow::Result<Response<Body>>) {
    match result {
        Ok(resp) => log(Level::Debug, "http-api", &format!("Response: {}", resp.status())),
        Err(e) => log(Level::Error, "http-api", &format!("Error: {}", e)),
    }
}
