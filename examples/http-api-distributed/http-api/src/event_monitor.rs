use crate::bindings::custom::event_monitor::watcher;
use crate::bindings::custom::event_monitor::types::WatchableResource;
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::helpers;
use crate::templates;

use serde::Deserialize;
use wstd::http::{Body, Request, Response, StatusCode};

use crate::LOG_CTX;

const EVENT_MONITOR_HTML: &str = include_str!("../resources/event_monitor.html");

static EVENTS: std::sync::Mutex<Vec<EventLogEntry>> = std::sync::Mutex::new(Vec::new());
const MAX_EVENTS: usize = 100;

pub struct EventLogEntry {
    pub time: String,
    pub action: String,
    pub group: String,
    pub kind: String,
    pub name: String,
    pub namespace: Option<String>,
    pub version: String,
}

pub fn push_event(
    action: &str,
    group: &str,
    version: &str,
    kind: &str,
    name: &str,
    namespace: &Option<String>,
) {
    let time = format!("{:?}", std::time::SystemTime::now());
    let time_short = time.split('.').next().unwrap_or(&time).to_string();

    let mut list = EVENTS.lock().unwrap();
    list.push(EventLogEntry {
        time: time_short,
        action: action.to_string(),
        group: group.to_string(),
        version: version.to_string(),
        kind: kind.to_string(),
        name: name.to_string(),
        namespace: namespace.clone(),
    });
    while list.len() > MAX_EVENTS {
        list.remove(0);
    }
}

pub async fn home(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    helpers::html_response(templates::render(EVENT_MONITOR_HTML))
}

// --------------- Create Connection ---------------

#[derive(Deserialize)]
struct CreateRequest {
    #[serde(rename = "api-server-url")]
    api_server_url: String,
    token: String,
}

pub async fn create(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: CreateRequest = helpers::parse_json_body(&mut req).await?;

    log(
        Level::Info,
        LOG_CTX,
        &format!("EVENT MONITOR CREATE: url={}", body.api_server_url),
    );

    match watcher::create(&body.api_server_url, &body.token) {
        Ok(()) => helpers::text_response(StatusCode::OK, "Connected to cluster"),
        Err(e) => helpers::text_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to connect: {e}"),
        ),
    }
}

// --------------- List Resources ---------------

pub async fn list_resources(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    match watcher::list_all_resources() {
        Ok(resources) => {
            let entries: Vec<serde_json::Value> = resources
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "group": r.group,
                        "version": r.version,
                        "kind": r.kind,
                    })
                })
                .collect();
            helpers::json_response(
                serde_json::json!({ "resources": entries, "count": entries.len() }).to_string(),
            )
        }
        Err(e) => helpers::json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Failed to list resources: {e}"),
        ),
    }
}

// --------------- Watch Resources ---------------

#[derive(Deserialize)]
struct WatchResourcesRequest {
    resources: Vec<WatchResourceItem>,
}

#[derive(Deserialize)]
struct WatchResourceItem {
    group: String,
    version: String,
    kind: String,
}

pub async fn watch_resources(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let body: WatchResourcesRequest = helpers::parse_json_body(&mut req).await?;

    let resources: Vec<WatchableResource> = body
        .resources
        .iter()
        .map(|r| WatchableResource {
            group: r.group.clone(),
            version: r.version.clone(),
            kind: r.kind.clone(),
        })
        .collect();

    let count = resources.len();
    log(
        Level::Info,
        LOG_CTX,
        &format!("EVENT MONITOR WATCH: {count} resources"),
    );

    match watcher::watch_resources(&resources) {
        Ok(()) => helpers::text_response(
            StatusCode::OK,
            format!("Watching {count} resources"),
        ),
        Err(e) => helpers::text_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to watch: {e}"),
        ),
    }
}

// --------------- Unwatch ---------------

pub async fn unwatch_resources(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    match watcher::unwatch_resources() {
        Ok(()) => helpers::text_response(StatusCode::OK, "Watchers cancelled"),
        Err(e) => helpers::text_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to unwatch: {e}"),
        ),
    }
}

// --------------- Event Log ---------------

pub async fn clear_log(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    EVENTS.lock().unwrap().clear();
    log(Level::Info, LOG_CTX, "Event log cleared");
    helpers::text_response(StatusCode::OK, "Log cleared")
}

pub async fn get_log(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let list = EVENTS.lock().unwrap();
    let entries: Vec<serde_json::Value> = list
        .iter()
        .map(|e| {
            serde_json::json!({
                "time": e.time,
                "action": e.action,
                "gvk": format!("{}/{}/{}", e.group, e.version, e.kind),
                "name": e.name,
                "namespace": e.namespace,
            })
        })
        .collect();
    helpers::json_response(
        serde_json::json!({ "events": entries, "count": entries.len() }).to_string(),
    )
}
