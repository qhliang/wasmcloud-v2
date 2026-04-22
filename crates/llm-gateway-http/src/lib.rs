mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "llm-gateway-http",
        generate_all,
    });
}

mod completions;
mod helpers;
mod responses;
mod types;

use bindings::wasi::logging::logging::{Level, log};
use wstd::http::{Body, Request, Response, StatusCode};

const LOG_CTX: &str = "llm-gateway-http";

#[wstd::http_server]
async fn main(req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let path = req.uri().path();
    log(
        Level::Debug,
        LOG_CTX,
        &format!("Request: {} {}", req.method(), path),
    );

    match path {
        "/v1/chat/completions" => completions::handle(req).await,
        "/v1/messages" => responses::handle(req).await,
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("Not found\n".into())
            .map_err(Into::into),
    }
}
