use crate::LOG_CTX;
use crate::bindings::wasi::blobstore::blobstore;
use crate::bindings::wasi::blobstore::types::{IncomingValue, OutgoingValue};
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::helpers;
use crate::templates;
use serde::Deserialize;
use wstd::http::{Body, Request, Response, StatusCode};
const R2_HTML: &str = include_str!("../resources/r2.html");
pub async fn home(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    helpers::html_response(templates::render(R2_HTML))
}

pub async fn containers(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    log(Level::Debug, LOG_CTX, "R2: listing containers");
    let containers = vec!["wasmcloud-v2".to_string()];
    if !blobstore::container_exists("wasmcloud-v2").unwrap_or(false) {
        let _ = blobstore::create_container("wasmcloud-v2");
    }
    let json = serde_json::to_string(&containers)?;
    helpers::json_response(json)
}
pub async fn container_create(req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let params = helpers::query_params(req.uri());
    let name = params
        .get("name")
        .ok_or_else(|| anyhow::anyhow!("missing container name"))?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("R2 CREATE CONTAINER: {}", name),
    );
    blobstore::create_container(name)
        .map_err(|e| anyhow::anyhow!("Failed to create container: {:?}", e))?;
    helpers::text_response(StatusCode::OK, "OK")
}
pub async fn container_delete(req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let params = helpers::query_params(req.uri());
    let name = params
        .get("name")
        .ok_or_else(|| anyhow::anyhow!("missing container name"))?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("R2 DELETE CONTAINER: {}", name),
    );
    blobstore::delete_container(name)
        .map_err(|e| anyhow::anyhow!("Failed to delete container: {:?}", e))?;
    helpers::text_response(StatusCode::OK, "OK")
}
pub async fn objects(req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let params = helpers::query_params(req.uri());
    let container_name = params
        .get("container")
        .ok_or_else(|| anyhow::anyhow!("missing container"))?;
    log(
        Level::Debug,
        LOG_CTX,
        &format!("R2 LIST OBJECTS: container={}", container_name),
    );
    let container = blobstore::get_container(container_name)
        .map_err(|e| anyhow::anyhow!("Failed to get container: {:?}", e))?;
    let stream = container
        .list_objects()
        .map_err(|e| anyhow::anyhow!("Failed to list objects: {:?}", e))?;
    let (objects, _) = stream
        .read_stream_object_names(1000)
        .map_err(|e| anyhow::anyhow!("Failed to read stream: {:?}", e))?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("R2 LIST OBJECTS OK: count={}", objects.len()),
    );
    let json = serde_json::to_string(&objects)?;
    helpers::json_response(json)
}
pub async fn object_get(req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let params = helpers::query_params(req.uri());
    let container_name = params
        .get("container")
        .ok_or_else(|| anyhow::anyhow!("missing container"))?;
    let object_name = params
        .get("name")
        .ok_or_else(|| anyhow::anyhow!("missing object name"))?;
    log(
        Level::Debug,
        LOG_CTX,
        &format!(
            "R2 GET OBJECT: container={}, name={}",
            container_name, object_name
        ),
    );
    let container = blobstore::get_container(container_name)
        .map_err(|e| anyhow::anyhow!("Failed to get container: {:?}", e))?;
    let incoming_value = container
        .get_data(object_name, 0, u64::MAX)
        .map_err(|e| anyhow::anyhow!("Failed to get object: {:?}", e))?;
    let data = IncomingValue::incoming_value_consume_sync(incoming_value)
        .map_err(|e| anyhow::anyhow!("Failed to consume value: {:?}", e))?;
    let value_str = String::from_utf8(data.clone())
        .unwrap_or_else(|_| format!("<binary data: {} bytes>", data.len()));
    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "R2 GET OBJECT OK: container={}, name={}, len={}",
            container_name,
            object_name,
            data.len()
        ),
    );
    helpers::text_response(StatusCode::OK, value_str)
}
#[derive(Deserialize)]
struct PutObjectRequest {
    container: String,
    name: String,
    data: String,
}
pub async fn object_put(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let put_req: PutObjectRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "R2 PUT OBJECT: container={}, name={}, data_len={}",
            put_req.container,
            put_req.name,
            put_req.data.len()
        ),
    );
    let container = blobstore::get_container(&put_req.container)
        .map_err(|e| anyhow::anyhow!("Failed to get container: {:?}", e))?;
    let outgoing_value = OutgoingValue::new_outgoing_value();
    let stream = outgoing_value
        .outgoing_value_write_body()
        .map_err(|e| anyhow::anyhow!("Failed to get write stream: {:?}", e))?;
    let data_bytes = put_req.data.as_bytes();
    let mut offset = 0;
    while offset < data_bytes.len() {
        let writable = stream
            .check_write()
            .map_err(|e| anyhow::anyhow!("check_write failed: {:?}", e))?;
        if writable == 0 {
            continue;
        }
        let end = (offset + writable as usize).min(data_bytes.len());
        stream
            .write(&data_bytes[offset..end])
            .map_err(|e| anyhow::anyhow!("write failed: {:?}", e))?;
        offset = end;
    }
    stream
        .flush()
        .map_err(|e| anyhow::anyhow!("flush failed: {:?}", e))?;
    container
        .write_data(&put_req.name, &outgoing_value)
        .map_err(|e| anyhow::anyhow!("Failed to write data: {:?}", e))?;
    OutgoingValue::finish(outgoing_value)
        .map_err(|e| anyhow::anyhow!("Failed to finish outgoing value: {:?}", e))?;
    log(
        Level::Debug,
        LOG_CTX,
        &format!(
            "R2 PUT OBJECT OK: container={}, name={}",
            put_req.container, put_req.name
        ),
    );
    helpers::text_response(StatusCode::OK, "OK")
}
pub async fn object_delete(req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let params = helpers::query_params(req.uri());
    let container_name = params
        .get("container")
        .ok_or_else(|| anyhow::anyhow!("missing container"))?;
    let object_name = params
        .get("name")
        .ok_or_else(|| anyhow::anyhow!("missing object name"))?;
    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "R2 DELETE OBJECT: container={}, name={}",
            container_name, object_name
        ),
    );
    let container = blobstore::get_container(container_name)
        .map_err(|e| anyhow::anyhow!("Failed to get container: {:?}", e))?;
    container
        .delete_object(object_name)
        .map_err(|e| anyhow::anyhow!("Failed to delete object: {:?}", e))?;
    log(
        Level::Debug,
        LOG_CTX,
        &format!(
            "R2 DELETE OBJECT OK: container={}, name={}",
            container_name, object_name
        ),
    );
    helpers::text_response(StatusCode::OK, "OK")
}
