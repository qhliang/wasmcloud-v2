use crate::bindings::custom::crontab::scheduler;
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::helpers;
use crate::templates;

use serde::Deserialize;
use wstd::http::{Body, Request, Response, StatusCode};

use crate::LOG_CTX;

const CRONTAB_HTML: &str = include_str!("../resources/crontab.html");

/// In-memory callback log entries (max 50).
static CALLBACKS: std::sync::Mutex<Vec<CallbackEntry>> = std::sync::Mutex::new(Vec::new());
const MAX_CALLBACKS: usize = 50;

struct CallbackEntry {
    time: String,
    message: String,
}

pub fn push_callback(message: String) {
    let time = format!("{:?}", std::time::SystemTime::now());
    let mut list = CALLBACKS.lock().unwrap();
    list.push(CallbackEntry { time, message });
    while list.len() > MAX_CALLBACKS {
        list.remove(0);
    }
}

pub async fn home(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    helpers::html_response(templates::render(CRONTAB_HTML))
}

// --------------- Schedule (cron) ---------------

#[derive(Deserialize)]
struct ScheduleRequest {
    name: String,
    #[serde(rename = "cron-expression")]
    cron_expression: String,
}

pub async fn schedule(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: ScheduleRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "CRONTAB SCHEDULE: name={}, expr={}",
            body.name, body.cron_expression
        ),
    );

    match scheduler::schedule(&body.name, &body.cron_expression) {
        Ok(()) => {
            log(
                Level::Info,
                LOG_CTX,
                &format!("CRONTAB SCHEDULE OK: name={}", body.name),
            );
            helpers::text_response(StatusCode::OK, "Schedule created successfully")
        }
        Err(e) => {
            let msg = format!("Schedule error: {:?}", e);
            log(Level::Error, LOG_CTX, &msg);
            helpers::text_response(StatusCode::BAD_REQUEST, msg)
        }
    }
}

// --------------- Schedule-delay (one-shot) ---------------

#[derive(Deserialize)]
struct ScheduleDelayRequest {
    name: String,
    #[serde(rename = "delay-ms")]
    delay_ms: u64,
}

pub async fn schedule_delay(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: ScheduleDelayRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "CRONTAB SCHEDULE-DELAY: name={}, delay_ms={}",
            body.name, body.delay_ms
        ),
    );

    match scheduler::schedule_delay(&body.name, body.delay_ms) {
        Ok(()) => {
            log(
                Level::Info,
                LOG_CTX,
                &format!("CRONTAB SCHEDULE-DELAY OK: name={}", body.name),
            );
            helpers::text_response(StatusCode::OK, "Delay schedule created successfully")
        }
        Err(e) => {
            let msg = format!("Schedule-delay error: {:?}", e);
            log(Level::Error, LOG_CTX, &msg);
            helpers::text_response(StatusCode::BAD_REQUEST, msg)
        }
    }
}

// --------------- Remove ---------------

pub async fn remove(req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let params = helpers::query_params(req.uri());
    let name = params
        .get("name")
        .ok_or_else(|| anyhow::anyhow!("missing name"))?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("CRONTAB REMOVE: name={}", name),
    );

    match scheduler::remove(name) {
        Ok(()) => {
            log(
                Level::Info,
                LOG_CTX,
                &format!("CRONTAB REMOVE OK: name={}", name),
            );
            helpers::text_response(StatusCode::OK, "Schedule removed")
        }
        Err(e) => {
            let msg = format!("Remove error: {:?}", e);
            log(Level::Error, LOG_CTX, &msg);
            helpers::text_response(StatusCode::BAD_REQUEST, msg)
        }
    }
}

// --------------- List ---------------

pub async fn list(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    log(Level::Debug, LOG_CTX, "CRONTAB LIST");
    match scheduler::list_schedules() {
        Ok(names) => {
            log(
                Level::Info,
                LOG_CTX,
                &format!("CRONTAB LIST OK: count={}", names.len()),
            );
            helpers::json_response(serde_json::to_string(&names)?)
        }
        Err(e) => {
            let msg = format!("List error: {:?}", e);
            log(Level::Error, LOG_CTX, &msg);
            helpers::text_response(StatusCode::BAD_REQUEST, msg)
        }
    }
}

// --------------- Callback (called by crontab service) ---------------

pub async fn callback(req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let params = helpers::query_params(req.uri());
    let name = params.get("name").cloned().unwrap_or_default();
    let message = format!("CRONTAB CALLBACK: schedule '{}' fired", name);

    log(Level::Info, LOG_CTX, &message);
    push_callback(message);

    helpers::text_response(StatusCode::OK, "OK")
}

// --------------- Callbacks JSON (for HTML polling) ---------------

pub async fn callbacks(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let list = CALLBACKS.lock().unwrap();
    let entries: Vec<serde_json::Value> = list
        .iter()
        .map(|e| {
            serde_json::json!({
                "time": e.time,
                "message": e.message,
            })
        })
        .collect();
    let count = entries.len();
    drop(list);
    helpers::json_response(
        serde_json::to_string(&serde_json::json!({
            "count": count,
            "entries": entries,
        }))
        .unwrap(),
    )
}
