use anyhow::Context as _;
use serde::de::DeserializeOwned;
use wstd::http::{Body, Response, StatusCode};

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
