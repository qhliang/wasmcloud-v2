use crate::bindings::custom::codex::executor;
use crate::bindings::custom::codex::session;
use crate::bindings::custom::codex::types::{ApprovalRequest, CodexEvent, ExecStreamEvent};
use crate::bindings::custom::wechat::sender;
use crate::bindings::wasi::clocks::monotonic_clock;
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::helpers;
use crate::templates;
use serde::Deserialize;
use wstd::http::{Body, Request, Response, StatusCode};

use crate::LOG_CTX;

const CODEX_HTML: &str = include_str!("../resources/codex.html");

/// Timeout in nanoseconds before sending a "processing" interim message (10 seconds).
const PROCESSING_TIMEOUT_NS: u64 = 10_000_000_000;

pub async fn home(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    helpers::html_response(templates::render(CODEX_HTML))
}

#[derive(Deserialize)]
struct ExecuteRequest {
    prompt: String,
    #[serde(default)]
    context_key: Option<String>,
}

/// Process a stream from codex execution, collecting events and usage.
fn process_stream(
    stream: executor::ExecStream,
) -> (Option<String>, Vec<serde_json::Value>, u64, u64) {
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
                        ExecStreamEvent::ApprovalNeeded(ApprovalRequest { item_id, command }) => {
                            all_events.push(serde_json::json!({
                                "type": "approval-needed",
                                "item_id": item_id,
                                "command": command,
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

    (session_id, all_events, total_input, total_output)
}

/// POST /codex/execute
/// Execute a task using codex and stream the results.
pub async fn execute(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: ExecuteRequest = helpers::parse_json_body(&mut req).await?;
    let context_key = body.context_key.as_deref().unwrap_or("default");

    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "CODEX EXECUTE: context_key={}, prompt_len={}",
            context_key,
            body.prompt.len()
        ),
    );

    match executor::execute(context_key, &body.prompt) {
        Ok(stream) => {
            let (session_id, all_events, total_input, total_output) = process_stream(stream);

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
            let (session_id, all_events, total_input, total_output) = process_stream(stream);

            let mut result = serde_json::json!({
                "events": all_events,
                "usage": {
                    "input_tokens": total_input,
                    "output_tokens": total_output,
                },
            });
            if let Some(sid) = session_id {
                result["session_id"] = serde_json::Value::String(sid);
            }

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

#[derive(Deserialize)]
struct NewSessionRequest {
    prompt: String,
    #[serde(default)]
    context_key: Option<String>,
}

/// POST /codex/new
/// Force create a new session and set it as current for the context key.
pub async fn new_session(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: NewSessionRequest = helpers::parse_json_body(&mut req).await?;
    let context_key = body.context_key.as_deref().unwrap_or("default");

    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "CODEX NEW SESSION: context_key={}, prompt_len={}",
            context_key,
            body.prompt.len()
        ),
    );

    match session::new_session(context_key, &body.prompt) {
        Ok(stream) => {
            let (session_id, all_events, total_input, total_output) = process_stream(stream);

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
                &format!("CODEX NEW SESSION ERROR: {:?}", e),
            );
            helpers::json_error(StatusCode::BAD_REQUEST, &format!("{:?}", e))
        }
    }
}

#[derive(Deserialize)]
struct ChangeSessionRequest {
    session_id: String,
    #[serde(default)]
    context_key: Option<String>,
}

/// POST /codex/change
/// Switch the current session for a context key.
pub async fn change_session(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: ChangeSessionRequest = helpers::parse_json_body(&mut req).await?;
    let context_key = body.context_key.as_deref().unwrap_or("default");

    match session::change_session(context_key, &body.session_id) {
        Ok(()) => helpers::json_response(serde_json::to_string(&serde_json::json!({
            "success": true,
            "context_key": context_key,
            "session_id": body.session_id,
        }))?), Err(e) => {
            log(
                Level::Error,
                LOG_CTX,
                &format!("CODEX CHANGE SESSION ERROR: {:?}", e),
            );
            helpers::json_error(StatusCode::BAD_REQUEST, &format!("{:?}", e))
        }
    }
}

#[derive(Deserialize)]
struct DeleteSessionRequest {
    session_id: String,
}

/// POST /codex/delete
/// Delete a session by ID.
pub async fn delete_session(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: DeleteSessionRequest = helpers::parse_json_body(&mut req).await?;

    match session::delete_session(&body.session_id) {
        Ok(()) => helpers::json_response(serde_json::to_string(&serde_json::json!({
            "success": true,
            "session_id": body.session_id,
        }))?), Err(e) => {
            log(
                Level::Error,
                LOG_CTX,
                &format!("CODEX DELETE SESSION ERROR: {:?}", e),
            );
            helpers::json_error(StatusCode::BAD_REQUEST, &format!("{:?}", e))
        }
    }
}

/// GET /codex/list
/// List all sessions.
pub async fn list_sessions(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    match session::list_sessions() {
        Ok(sessions) => {
            let list: Vec<serde_json::Value> = sessions
                .iter()
                .map(|s| {
                    let mut obj = serde_json::json!({
                        "session_id": s.session_id,
                        "thread_id": s.thread_id,
                        "created_at": s.created_at,
                    });
                    if let Some(ref usage) = s.token_usage {
                        obj["token_usage"] = serde_json::json!({
                            "input_tokens": usage.input_tokens,
                            "cached_input_tokens": usage.cached_input_tokens,
                            "output_tokens": usage.output_tokens,
                        });
                    }
                    obj
                })
                .collect();
            helpers::json_response(serde_json::to_string(&serde_json::json!({
                "sessions": list,
            }))?)
        }
        Err(e) => {
            log(
                Level::Error,
                LOG_CTX,
                &format!("CODEX LIST SESSIONS ERROR: {:?}", e),
            );
            helpers::json_error(StatusCode::BAD_REQUEST, &format!("{:?}", e))
        }
    }
}

/// Execute a prompt via Codex for chat contexts (e.g. WeChat).
/// Uses `sender_id` as the context key to maintain per-user sessions.
/// Sends an interim "processing" message if execution takes longer than 10 seconds.
/// Returns the collected text content from the Codex response.
pub fn execute_for_chat(sender_id: &str, prompt: &str) -> Result<String, String> {
    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "CODEX CHAT: sender={}, prompt_len={}",
            sender_id,
            prompt.len()
        ),
    );

    let stream = executor::execute(sender_id, prompt).map_err(|e| format!("{e:?}"))?;

    let start = monotonic_clock::now();
    let mut texts: Vec<String> = Vec::new();
    let mut sent_processing = false;

    loop {
        match stream.next() {
            Ok((events, ended)) => {
                for event in &events {
                    match event {
                        ExecStreamEvent::Event(CodexEvent {
                            text_content: Some(text),
                            ..
                        }) => {
                            texts.push(text.clone());
                        }
                        ExecStreamEvent::Event(_) => {}
                        ExecStreamEvent::Done(_) => {}
                        ExecStreamEvent::Usage(_) => {}
                        ExecStreamEvent::Error(err) => {
                            log(
                                Level::Error,
                                LOG_CTX,
                                &format!("CODEX CHAT STREAM ERROR: {err}"),
                            );
                        }
                        ExecStreamEvent::ApprovalNeeded(_) => {
                            // Auto-approve in chat mode (auto_approve=true by default)
                        }
                    }
                }

                if !ended && !sent_processing {
                    let elapsed = monotonic_clock::now() - start;
                    if elapsed >= PROCESSING_TIMEOUT_NS {
                        let _ = sender::send_text(sender_id, "正在处理中，请稍候...");
                        sent_processing = true;
                        log(Level::Info, LOG_CTX, "CODEX CHAT: sent processing notice");
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
                    &format!("CODEX CHAT STREAM FATAL: {e:?}"),
                );
                break;
            }
        }
    }

    let result = if texts.is_empty() {
        String::from("(无输出)")
    } else {
        texts.join("\n")
    };

    log(
        Level::Info,
        LOG_CTX,
        &format!("CODEX CHAT DONE: sender={}, result_len={}", sender_id, result.len()),
    );

    Ok(result)
}

#[derive(Deserialize)]
struct SetAutoApproveRequest {
    session_id: String,
    auto_approve: bool,
}

/// POST /codex/set-auto-approve
/// Toggle auto-approve mode for a session.
pub async fn set_auto_approve(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: SetAutoApproveRequest = helpers::parse_json_body(&mut req).await?;

    match session::set_auto_approve(&body.session_id, body.auto_approve) {
        Ok(()) => helpers::json_response(serde_json::to_string(&serde_json::json!({
            "success": true,
            "session_id": body.session_id,
            "auto_approve": body.auto_approve,
        }))?),
        Err(e) => {
            log(
                Level::Error,
                LOG_CTX,
                &format!("CODEX SET AUTO APPROVE ERROR: {:?}", e),
            );
            helpers::json_error(StatusCode::BAD_REQUEST, &format!("{:?}", e))
        }
    }
}

#[derive(Deserialize)]
struct ApproveRequest {
    session_id: String,
    item_id: String,
    approved: bool,
}

/// POST /codex/approve
/// Approve or deny a pending command execution.
pub async fn approve(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: ApproveRequest = helpers::parse_json_body(&mut req).await?;

    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "CODEX APPROVE: session_id={}, item_id={}, approved={}",
            body.session_id, body.item_id, body.approved
        ),
    );

    match session::approve(&body.session_id, &body.item_id, body.approved) {
        Ok(()) => helpers::json_response(serde_json::to_string(&serde_json::json!({
            "success": true,
            "session_id": body.session_id,
            "item_id": body.item_id,
            "approved": body.approved,
        }))?),
        Err(e) => {
            log(
                Level::Error,
                LOG_CTX,
                &format!("CODEX APPROVE ERROR: {:?}", e),
            );
            helpers::json_error(StatusCode::BAD_REQUEST, &format!("{:?}", e))
        }
    }
}
