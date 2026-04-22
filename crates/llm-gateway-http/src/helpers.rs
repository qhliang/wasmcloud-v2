use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context as _;
use serde::de::DeserializeOwned;
use wstd::http::{Body, Response, StatusCode};

static ID_COUNTER: AtomicU64 = AtomicU64::new(1);

pub fn generate_id(prefix: &str) -> String {
    let id = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}{id:016x}")
}

pub fn json_response(
    status: StatusCode,
    body: &impl serde::Serialize,
) -> anyhow::Result<Response<Body>> {
    let json = serde_json::to_string(body).context("failed to serialize response")?;
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(json.into())
        .map_err(Into::into)
}

pub async fn parse_json_body<T: DeserializeOwned>(
    req: &mut wstd::http::Request<Body>,
) -> anyhow::Result<T> {
    req.body_mut()
        .json()
        .await
        .context("failed to parse request body")
}

pub fn map_error_status(e: &crate::bindings::custom::llm_gateway::types::LlmError) -> StatusCode {
    use crate::bindings::custom::llm_gateway::types::LlmError;
    match e {
        LlmError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
        LlmError::AuthenticationError(_) => StatusCode::UNAUTHORIZED,
        LlmError::ProviderError(_) => StatusCode::BAD_GATEWAY,
        LlmError::RateLimitError(_) => StatusCode::TOO_MANY_REQUESTS,
        LlmError::ModelNotFound(_) => StatusCode::NOT_FOUND,
        LlmError::Unexpected(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub fn map_error_type(e: &crate::bindings::custom::llm_gateway::types::LlmError) -> String {
    use crate::bindings::custom::llm_gateway::types::LlmError;
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
