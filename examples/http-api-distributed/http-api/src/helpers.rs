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
    uri.query().map(|q| urlencoded_parse(q)).unwrap_or_default()
}

pub async fn parse_json_body<T: DeserializeOwned>(req: &mut Request<Body>) -> anyhow::Result<T> {
    req.body_mut().json().await.context("failed to parse body")
}

// ============ URL encoding ============

fn urlencoded_parse(query: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for pair in query.split('&') {
        if let Some(eq) = pair.find('=') {
            let key = urlencoding_decode(&pair[..eq]);
            let value = urlencoding_decode(&pair[eq + 1..]);
            map.insert(key, value);
        } else if !pair.is_empty() {
            map.insert(pair.to_string(), String::new());
        }
    }
    map
}

fn urlencoding_decode(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                result.push(byte as char);
            } else {
                result.push('%');
                result.push_str(&hex);
            }
        } else if c == '+' {
            result.push(' ');
        } else {
            result.push(c);
        }
    }
    result
}
