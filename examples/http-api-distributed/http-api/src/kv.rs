use crate::LOG_CTX;
use crate::bindings::wasi::keyvalue::store;
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::helpers;
use crate::templates;
use serde::Deserialize;
use wstd::http::{Body, Request, Response, StatusCode};

const KV_HTML: &str = include_str!("../resources/kv.html");
const DEFAULT_BUCKET: &str = "default";

pub async fn home(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    helpers::html_response(templates::render(KV_HTML))
}

fn get_bucket() -> Result<store::Bucket, String> {
    store::open(DEFAULT_BUCKET).map_err(|e| {
        let err_msg = format!("Failed to open bucket: {:?}", e);
        log(Level::Error, LOG_CTX, &err_msg);
        err_msg
    })
}

#[derive(Deserialize)]
struct SetRequest {
    key: String,
    value: String,
}
pub async fn set(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let set_req: SetRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "KV SET: key={}, value_len={}",
            set_req.key,
            set_req.value.len()
        ),
    );
    let bucket = get_bucket().map_err(|e| anyhow::anyhow!(e))?;
    let value = set_req.value.into_bytes();
    bucket
        .set(&set_req.key, &value)
        .map_err(|e| anyhow::anyhow!("Failed to set value: {:?}", e))?;
    log(
        Level::Debug,
        LOG_CTX,
        &format!("KV SET OK: key={}", set_req.key),
    );
    helpers::text_response(StatusCode::OK, "OK")
}
pub async fn get(req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let params = helpers::query_params(req.uri());
    let key = params
        .get("key")
        .ok_or_else(|| anyhow::anyhow!("missing key"))?;
    log(Level::Debug, LOG_CTX, &format!("KV GET: key={}", key));
    let bucket = get_bucket().map_err(|e| anyhow::anyhow!(e))?;
    match bucket.get(key) {
        Ok(Some(value)) => {
            log(
                Level::Info,
                LOG_CTX,
                &format!("KV GET OK: key={}, len={}", key, value.len()),
            );
            let value_str =
                String::from_utf8(value).unwrap_or_else(|_| "<binary data>".to_string());
            helpers::text_response(StatusCode::OK, value_str)
        }
        Ok(None) => {
            log(
                Level::Info,
                LOG_CTX,
                &format!("KV GET NOT FOUND: key={}", key),
            );
            helpers::text_response(StatusCode::NOT_FOUND, "Key not found")
        }
        Err(e) => {
            log(
                Level::Error,
                LOG_CTX,
                &format!("KV GET ERROR: key={}, error={:?}", key, e),
            );
            helpers::text_response(StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {:?}", e))
        }
    }
}
pub async fn delete(req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let params = helpers::query_params(req.uri());
    let key = params
        .get("key")
        .ok_or_else(|| anyhow::anyhow!("missing key"))?;
    log(Level::Info, LOG_CTX, &format!("KV DELETE: key={}", key));
    let bucket = get_bucket().map_err(|e| anyhow::anyhow!(e))?;
    bucket
        .delete(key)
        .map_err(|e| anyhow::anyhow!("Failed to delete: {:?}", e))?;
    log(Level::Debug, LOG_CTX, &format!("KV DELETE OK: key={}", key));
    helpers::text_response(StatusCode::OK, "OK")
}
pub async fn keys(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    log(Level::Debug, LOG_CTX, "KV KEYS: listing all keys");
    let bucket = get_bucket().map_err(|e| anyhow::anyhow!(e))?;
    let response = bucket
        .list_keys(None)
        .map_err(|e| anyhow::anyhow!("Failed to list keys: {:?}", e))?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("KV KEYS OK: count={}", response.keys.len()),
    );
    let keys_json = serde_json::to_string(&response.keys)?;
    helpers::json_response(keys_json)
}
