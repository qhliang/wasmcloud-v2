use crate::bindings::custom::workflow::manager;
use crate::bindings::custom::workflow::types::VarPair;
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::helpers;
use crate::templates;

use serde::Deserialize;
use wstd::http::{Body, Request, Response, StatusCode};

use crate::LOG_CTX;

const WORKFLOW_HTML: &str = include_str!("../resources/workflow.html");

pub async fn home(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    helpers::html_response(templates::render(WORKFLOW_HTML))
}

// --------------- Start Workflow ---------------

#[derive(Deserialize)]
struct StartRequest {
    #[serde(rename = "workflow-def")]
    workflow_def: String,
    #[serde(default)]
    vars: Vec<VarItem>,
}

#[derive(Deserialize)]
struct VarItem {
    key: String,
    value: String,
}

pub async fn start(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: StartRequest = helpers::parse_json_body(&mut req).await?;

    let pairs: Vec<VarPair> = body
        .vars
        .iter()
        .map(|v| VarPair {
            key: v.key.clone(),
            value: v.value.clone(),
        })
        .collect();

    log(Level::Info, LOG_CTX, "WORKFLOW START");

    match manager::start(&body.workflow_def, &pairs) {
        Ok(pid) => helpers::json_response(serde_json::json!({ "pid": pid }).to_string()),
        Err(e) => helpers::json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Failed to start: {e}"),
        ),
    }
}

// --------------- List Processes ---------------

pub async fn list(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    match manager::list_processes() {
        Ok(procs) => {
            let entries: Vec<serde_json::Value> = procs
                .iter()
                .map(|p| {
                    serde_json::json!({
                        "pid": p.pid,
                        "mid": p.mid,
                        "state": p.state,
                        "start_time": p.start_time,
                        "end_time": p.end_time,
                    })
                })
                .collect();
            helpers::json_response(
                serde_json::json!({ "processes": entries, "count": entries.len() }).to_string(),
            )
        }
        Err(e) => helpers::json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Failed to list: {e}"),
        ),
    }
}

// --------------- Process Status ---------------

#[derive(Deserialize)]
struct StatusRequest {
    pid: String,
}

pub async fn status(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: StatusRequest = helpers::parse_json_body(&mut req).await?;

    match manager::process_status(&body.pid) {
        Ok(info) => helpers::json_response(
            serde_json::json!({
                "pid": info.pid,
                "mid": info.mid,
                "state": info.state,
                "start_time": info.start_time,
                "end_time": info.end_time,
            })
            .to_string(),
        ),
        Err(e) => helpers::json_error(StatusCode::NOT_FOUND, &e.to_string()),
    }
}
