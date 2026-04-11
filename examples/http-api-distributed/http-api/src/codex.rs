use crate::bindings::custom::codex::executor;
use crate::bindings::custom::codex::session;
use crate::bindings::custom::codex::types::{CodexEvent, ExecStreamEvent};
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::helpers;
use crate::templates;
use serde::Deserialize;
use wstd::http::{Body, Request, Response, StatusCode};

use crate::LOG_CTX;

const CODEX_HTML: &str = include_str!("../resources/codex.html");

pub async fn home(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    helpers::html_response(templates::render(CODEX_HTML))
}

#[derive(Deserialize)]
struct ExecuteRequest {
    prompt: String,
}

/// POST /codex/execute
/// Execute a task using codex and stream the results.
pub async fn execute(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: ExecuteRequest = helpers::parse_json_body(&mut req).await?;

    log(
        Level::Info,
        LOG_CTX,
        &format!("CODEX EXECUTE: prompt_len={}", body.prompt.len()),
    );

    match executor::execute(&body.prompt) {
        Ok(stream) => {
            let mut all_events: Vec<serde_json::Value> = Vec::new();
            let mut session_id: Option<String> = None;
            let mut total_input: u64 = 0;
            let mut total_output: u64 = 0;

            loop {
                match stream.next() {
                    Ok((events, ended)) => {
                        for event in &events {
                            match event {
                                ExecStreamEvent::Done(sid) => {
                                    session_id = Some(sid.clone());
                                }
                                ExecStreamEvent::Event(CodexEvent {
                                    event_type,
                                    item_type,
                                    text_content,
                                    command,
                                    raw_json,
                                    ..
                                }) => {
                                    let mut evt = serde_json::json!({
                                        "type": event_type,
                                    });
                                    if let Some(it) = item_type {
                                        evt["item_type"] = serde_json::Value::String(it.clone());
                                    }
                                    if let Some(tc) = text_content {
                                        evt["text"] = serde_json::Value::String(tc.clone());
                                    }
                                    if let Some(cmd) = command {
                                        evt["command"] = serde_json::Value::String(cmd.clone());
                                    }
                                    let _ = raw_json;
                                    all_events.push(evt);
                                }
                                ExecStreamEvent::Usage(usage) => {
                                    total_input += usage.input_tokens;
                                    total_output += usage.output_tokens;
                                }
                                ExecStreamEvent::Error(err) => {
                                    all_events.push(serde_json::json!({
                                        "type": "error",
                                        "message": err,
                                    }));
                                }
                            }
                        }
                        if ended {
                            break;
                        }
                    }
                    Err(e) => {
                        log(
                            Level::Error,
                            LOG_CTX,
                            &format!("CODEX STREAM ERROR: {:?}", e),
                        );
                        all_events.push(serde_json::json!({
                            "type": "error",
                            "message": format!("{:?}", e),
                        }));
                        break;
                    }
                }
            }

            let result = serde_json::json!({
                "session_id": session_id,
                "events": all_events,
                "usage": {
                    "input_tokens": total_input,
                    "output_tokens": total_output,
                },
            });

            helpers::json_response(serde_json::to_string(&result)?)
        }
        Err(e) => {
            log(
                Level::Error,
                LOG_CTX,
                &format!("CODEX EXECUTE ERROR: {:?}", e),
            );
            helpers::json_error(StatusCode::BAD_REQUEST, &format!("{:?}", e))
        }
    }
}

#[derive(Deserialize)]
struct UsageRequest {
    session_id: String,
}

/// POST /codex/usage
/// Get token usage for a session.
pub async fn get_usage(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: UsageRequest = helpers::parse_json_body(&mut req).await?;

    match session::get_usage(&body.session_id) {
        Ok(usage) => {
            let result = serde_json::json!({
                "input_tokens": usage.input_tokens,
                "cached_input_tokens": usage.cached_input_tokens,
                "output_tokens": usage.output_tokens,
            });
            helpers::json_response(serde_json::to_string(&result)?)
        }
        Err(e) => {
            log(
                Level::Error,
                LOG_CTX,
                &format!("CODEX USAGE ERROR: {:?}", e),
            );
            helpers::json_error(StatusCode::BAD_REQUEST, &format!("{:?}", e))
        }
    }
}

#[derive(Deserialize)]
struct ResumeRequest {
    session_id: String,
    prompt: String,
}

/// POST /codex/resume
/// Resume a session with a follow-up prompt.
pub async fn resume(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: ResumeRequest = helpers::parse_json_body(&mut req).await?;

    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "CODEX RESUME: session_id={}, prompt_len={}",
            body.session_id,
            body.prompt.len()
        ),
    );

    match session::resume(&body.session_id, &body.prompt) {
        Ok(stream) => {
            let mut all_events: Vec<serde_json::Value> = Vec::new();
            let mut total_input: u64 = 0;
            let mut total_output: u64 = 0;

            loop {
                match stream.next() {
                    Ok((events, ended)) => {
                        for event in &events {
                            match event {
                                ExecStreamEvent::Done(sid) => {
                                    all_events.push(serde_json::json!({
                                        "type": "done",
                                        "session_id": sid,
                                    }));
                                }
                                ExecStreamEvent::Event(CodexEvent {
                                    event_type,
                                    item_type,
                                    text_content,
                                    command,
                                    raw_json,
                                    ..
                                }) => {
                                    let mut evt = serde_json::json!({
                                        "type": event_type,
                                    });
                                    if let Some(it) = item_type {
                                        evt["item_type"] = serde_json::Value::String(it.clone());
                                    }
                                    if let Some(tc) = text_content {
                                        evt["text"] = serde_json::Value::String(tc.clone());
                                    }
                                    if let Some(cmd) = command {
                                        evt["command"] = serde_json::Value::String(cmd.clone());
                                    }
                                    let _ = raw_json;
                                    all_events.push(evt);
                                }
                                ExecStreamEvent::Usage(usage) => {
                                    total_input += usage.input_tokens;
                                    total_output += usage.output_tokens;
                                }
                                ExecStreamEvent::Error(err) => {
                                    all_events.push(serde_json::json!({
                                        "type": "error",
                                        "message": err,
                                    }));
                                }
                            }
                        }
                        if ended {
                            break;
                        }
                    }
                    Err(e) => {
                        log(
                            Level::Error,
                            LOG_CTX,
                            &format!("CODEX RESUME STREAM ERROR: {:?}", e),
                        );
                        break;
                    }
                }
            }

            let result = serde_json::json!({
                "events": all_events,
                "usage": {
                    "input_tokens": total_input,
                    "output_tokens": total_output,
                },
            });

            helpers::json_response(serde_json::to_string(&result)?)
        }
        Err(e) => {
            log(
                Level::Error,
                LOG_CTX,
                &format!("CODEX RESUME ERROR: {:?}", e),
            );
            helpers::json_error(StatusCode::BAD_REQUEST, &format!("{:?}", e))
        }
    }
}
