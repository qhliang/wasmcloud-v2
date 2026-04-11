use crate::LOG_CTX;
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::bindings::wasmcloud::messaging::consumer;
use crate::helpers;

use serde::Deserialize;
use wstd::http::{Body, Request, Response, StatusCode};
use wstd::time::Duration;

#[derive(Deserialize)]
struct TaskRequest {
    worker: Option<String>,
    payload: String,
}

pub async fn create(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let task_request: TaskRequest = helpers::parse_json_body(&mut req).await?;

    let worker = task_request.worker.unwrap_or_else(|| "default".to_string());
    let subject = format!("tasks.{}", worker);

    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "Creating task: worker={}, payload_len={}",
            worker,
            task_request.payload.len()
        ),
    );

    let body = task_request.payload.into_bytes();
    let request_timeout = Duration::from_secs(5).as_millis() as u32;

    match consumer::request(&subject, &body, request_timeout) {
        Ok(resp) => {
            log(
                Level::Info,
                LOG_CTX,
                &format!(
                    "Task completed: subject={}, response_len={}",
                    subject,
                    resp.body.len()
                ),
            );
            helpers::text_response(StatusCode::OK, resp.body)
        }
        Err(err) => {
            log(
                Level::Error,
                LOG_CTX,
                &format!("Task failed: subject={}, error={}", subject, err),
            );
            helpers::text_response(StatusCode::BAD_GATEWAY, err)
        }
    }
}
