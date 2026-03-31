mod bindings {
    wit_bindgen::generate!({
        path: "../wit",
        world: "http-api",
        generate_all,
    });

    use super::CrontabHandler;

    export!(CrontabHandler);
}

mod crontab;
mod d1;
mod helpers;
mod kv;
mod llm;
mod r2;
mod task;
mod templates;

use bindings::exports::custom::crontab::handler::Guest;
use bindings::wasi::logging::logging::{Level, log};
use wstd::http::{Body, Request, Response, StatusCode};

const LOG_CTX: &str = "http-api";

static HOME_HTML: &str = include_str!("../resources/home.html");

struct CrontabHandler;

impl Guest for CrontabHandler {
    fn handle_tick(name: String) -> Result<(), String> {
        let message = format!(
            "CRONTAB TICK: schedule '{}' fired via handle-tick export",
            name
        );
        log(Level::Info, LOG_CTX, &message);
        crontab::push_callback(message);
        Ok(())
    }
}

#[wstd::http_server]
async fn main(req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let path = req.uri().path();
    log(
        Level::Debug,
        LOG_CTX,
        &format!("Request: {} {}", req.method(), path),
    );

    let result = match path {
        "/" => helpers::html_response(templates::render(HOME_HTML)),
        "/task" => task::create(req).await,
        "/kv" | "/kv/" => kv::home(req).await,
        "/kv/get" => kv::get(req).await,
        "/kv/set" => kv::set(req).await,
        "/kv/delete" => kv::delete(req).await,
        "/kv/keys" => kv::keys(req).await,
        "/d1" | "/d1/" => d1::home(req).await,
        "/d1/query" => d1::query(req).await,
        "/d1/batch" => d1::batch(req).await,
        "/r2" | "/r2/" => r2::home(req).await,
        "/r2/containers" => r2::containers(req).await,
        "/r2/container/create" => r2::container_create(req).await,
        "/r2/container/delete" => r2::container_delete(req).await,
        "/r2/objects" => r2::objects(req).await,
        "/r2/object/get" => r2::object_get(req).await,
        "/r2/object/put" => r2::object_put(req).await,
        "/r2/object/delete" => r2::object_delete(req).await,
        "/llm" | "/llm/" => llm::home(req).await,
        "/llm/chat" => llm::chat(req).await,
        "/crontab" | "/crontab/" => crontab::home(req).await,
        "/crontab/schedule" => crontab::schedule(req).await,
        "/crontab/schedule-delay" => crontab::schedule_delay(req).await,
        "/crontab/remove" => crontab::remove(req).await,
        "/crontab/list" => crontab::list(req).await,
        "/crontab/callback" => crontab::callback(req).await,
        "/crontab/callbacks" => crontab::callbacks(req).await,
        _ => {
            log(Level::Debug, LOG_CTX, &format!("Not found: {}", path));
            helpers::text_response(StatusCode::NOT_FOUND, "Not found\n")
        }
    };

    match &result {
        Ok(resp) => log(
            Level::Debug,
            LOG_CTX,
            &format!("Response: {}", resp.status()),
        ),
        Err(e) => log(Level::Error, LOG_CTX, &format!("Error: {}", e)),
    }
    result
}
