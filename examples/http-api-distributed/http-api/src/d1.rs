use crate::LOG_CTX;
use crate::bindings::custom::cf_d1::query::D1Client;
use crate::bindings::custom::cf_d1::types::{ColumnValue, QueryError};
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::helpers;
use crate::templates;
use serde::Deserialize;
use wstd::http::{Body, Request, Response, StatusCode};

const D1_HTML: &str = include_str!("../resources/d1.html");

pub async fn home(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    helpers::html_response(templates::render(D1_HTML))
}

#[derive(Deserialize)]
struct D1QueryRequest {
    sql: String,
    params: Vec<JsonValue>,
}

#[derive(Deserialize)]
struct D1BatchRequest {
    sql: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum JsonValue {
    Null,
    Bool(bool),
    Number(i64),
    Float(f64),
    String(String),
}

impl JsonValue {
    fn to_column_value(&self) -> ColumnValue {
        match self {
            JsonValue::Null => ColumnValue::Null,
            JsonValue::Bool(b) => ColumnValue::Integer(if *b { 1 } else { 0 }),
            JsonValue::Number(n) => ColumnValue::Integer(*n),
            JsonValue::Float(f) => ColumnValue::Real(*f),
            JsonValue::String(s) => ColumnValue::Text(s.clone()),
        }
    }
}

fn column_value_to_json(val: &ColumnValue) -> serde_json::Value {
    match val {
        ColumnValue::Null => serde_json::Value::Null,
        ColumnValue::Integer(n) => serde_json::json!(n),
        ColumnValue::Real(f) => serde_json::json!(f),
        ColumnValue::Text(s) => serde_json::json!(s),
        ColumnValue::Blob(b) => serde_json::json!({ "blob": b.len() }),
    }
}

fn query_error_to_string(e: QueryError) -> String {
    match e {
        QueryError::InvalidQuery(s) => format!("Invalid query: {}", s),
        QueryError::InvalidParams(s) => format!("Invalid params: {}", s),
        QueryError::DatabaseError(s) => format!("Database error: {}", s),
        QueryError::ConnectionError(s) => format!("Connection error: {}", s),
        QueryError::Unexpected(s) => format!("Unexpected error: {}", s),
    }
}

pub async fn query(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let query_req: D1QueryRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!(
            "D1 QUERY: sql={}, params={}",
            query_req.sql,
            query_req.params.len()
        ),
    );

    let params: Vec<ColumnValue> = query_req
        .params
        .iter()
        .map(|p| p.to_column_value())
        .collect();

    let client = D1Client::new(None);
    match client.query(&query_req.sql, &params) {
        Ok(result) => {
            log(
                Level::Info,
                LOG_CTX,
                &format!(
                    "D1 QUERY OK: rows={}, affected={}",
                    result.rows.len(),
                    result.rows_affected
                ),
            );
            let json_result = serde_json::json!({
                "columns": result.columns.iter().map(|c| serde_json::json!({
                    "name": c.name,
                    "type": c.column_type
                })).collect::<Vec<_>>(),
                "rows": result.rows.iter().map(|row| {
                    serde_json::json!({
                        "values": row.values.iter().map(column_value_to_json).collect::<Vec<_>>()
                    })
                }).collect::<Vec<_>>(),
                "rows_affected": result.rows_affected,
                "last_insert_rowid": result.last_insert_rowid,
            });
            helpers::json_response(serde_json::to_string(&json_result)?)
        }
        Err(e) => {
            log(Level::Error, LOG_CTX, &format!("D1 QUERY ERROR: {:?}", e));
            helpers::json_error(StatusCode::BAD_REQUEST, &query_error_to_string(e))
        }
    }
}

pub async fn batch(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let batch_req: D1BatchRequest = helpers::parse_json_body(&mut req).await?;
    log(
        Level::Info,
        LOG_CTX,
        &format!("D1 BATCH: sql_len={}", batch_req.sql.len()),
    );

    let client = D1Client::new(None);
    match client.query_batch(&batch_req.sql) {
        Ok(results) => {
            log(
                Level::Info,
                LOG_CTX,
                &format!("D1 BATCH OK: {} statements(s) executed", results.len()),
            );
            let json_results: Vec<serde_json::Value> = results
                .iter()
                .map(|result| {
                    let columns: Vec<_> = result.columns.iter().map(|c| serde_json::json!({
                        "name": c.name,
                        "type": c.column_type
                    })).collect();
                    let rows: Vec<_> = result.rows.iter().map(|row| {
                        serde_json::json!({
                            "values": row.values.iter().map(column_value_to_json).collect::<Vec<_>>()
                        })
                    }).collect();
                    serde_json::json!({
                        "columns": columns,
                        "rows": rows,
                        "rows_affected": result.rows_affected,
                        "last_insert_rowid": result.last_insert_rowid,
                    })
                })
                .collect();
            helpers::json_response(serde_json::to_string(&json_results)?)
        }
        Err(e) => {
            log(Level::Error, LOG_CTX, &format!("D1 BATCH ERROR: {:?}", e));
            helpers::json_error(StatusCode::BAD_REQUEST, &query_error_to_string(e))
        }
    }
}
