//! # Cloudflare D1 Host Plugin (Resource-based)
//!
//! Two config sources with priority:
//! 1. Wasm dynamic config (passed via resource constructor)
//! 2. Static interface config (fallback from wasmcloud config)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use indexmap::IndexMap;
use opentelemetry::metrics::Counter;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::debug;
use wasmtime::component::Resource;

use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::WorkloadItem;
use wash_runtime::plugin::config::resolve_field;
use wash_runtime::plugin::{HostPlugin, WorkloadTracker};
use wash_runtime::wit::{WitInterface, WitWorld};

mod bindings {
    wasmtime::component::bindgen!({
        world: "d1",
        imports: {
            default: async | trappable | tracing,
        },
        with: {
            "custom:cf-d1/query.d1-client": super::D1ClientHandle,
        },
    });
}

use bindings::custom::cf_d1::types::{
    ColumnMeta, ColumnValue, D1Config, QueryError, QueryResult, ResultRow,
};

const PLUGIN_ID: &str = "cf-d1";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Host-side state for a d1-client resource instance.
pub struct D1ClientHandle {
    account_id: String,
    database_id: String,
    client: Client,
}

/// Per-component data.
struct ComponentData {
    /// Static interface config from wasmcloud config (fallback source)
    interface_config: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

struct CloudflareD1Metrics {
    queries_total: Counter<u64>,
}

impl Default for CloudflareD1Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl CloudflareD1Metrics {
    fn new() -> Self {
        let meter = opentelemetry::global::meter("cf-d1");
        let queries_total = meter
            .u64_counter("cf_d1_queries_total")
            .with_description("Total number of SQL queries executed on Cloudflare D1")
            .build();
        Self { queries_total }
    }
}

// ---------------------------------------------------------------------------
// Plugin struct
// ---------------------------------------------------------------------------

/// Cloudflare D1 SQL database plugin
#[derive(Clone)]
pub struct CloudflareD1 {
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
    metrics: Arc<CloudflareD1Metrics>,
}

impl Default for CloudflareD1 {
    fn default() -> Self {
        Self::new()
    }
}

impl CloudflareD1 {
    /// Create a new Cloudflare D1 plugin
    pub fn new() -> Self {
        Self {
            tracker: Arc::new(RwLock::new(WorkloadTracker::default())),
            metrics: Arc::new(CloudflareD1Metrics::new()),
        }
    }

    fn record_query(&self, operation: &str) {
        let attributes = [opentelemetry::KeyValue::new(
            "operation",
            operation.to_string(),
        )];
        self.metrics.queries_total.add(1, &attributes);
    }
}

// ---------------------------------------------------------------------------
// D1 API Request/Response Types
// ---------------------------------------------------------------------------

/// D1 API query request
#[derive(Serialize)]
struct D1QueryRequest {
    sql: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<Vec<serde_json::Value>>,
}

/// D1 API raw query request (for batch operations)
#[derive(Serialize)]
struct D1RawRequest {
    sql: String,
}

/// D1 API response (for /query endpoint - results is an array of objects)
#[derive(Deserialize)]
struct D1Response {
    result: Vec<D1Result>,
    success: bool,
    #[serde(default)]
    errors: Vec<D1Error>,
}

/// D1 query result (for /query endpoint - results is an array of row objects)
#[derive(Deserialize, Default)]
struct D1Result {
    #[serde(default)]
    results: Vec<IndexMap<String, serde_json::Value>>,
    #[serde(default)]
    meta: Option<D1Meta>,
}

/// D1 API response (for /raw endpoint - results has columns and rows arrays)
#[derive(Deserialize)]
struct D1ResponseRaw {
    result: Vec<D1ResultRaw>,
    success: bool,
    #[serde(default)]
    errors: Vec<D1Error>,
}

/// D1 query result (for /raw endpoint - results is an object with columns/rows)
#[derive(Deserialize, Default)]
struct D1ResultRaw {
    #[serde(default)]
    results: D1ResultRawInner,
    #[serde(default)]
    meta: Option<D1Meta>,
}

#[derive(Deserialize, Default)]
struct D1ResultRawInner {
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    rows: Vec<Vec<serde_json::Value>>,
}

/// D1 result metadata
#[derive(Deserialize, Default)]
#[allow(dead_code)]
struct D1Meta {
    #[serde(default)]
    changed_db: Option<bool>,
    #[serde(default)]
    changes: Option<u64>,
    #[serde(default)]
    duration: Option<f64>,
    #[serde(default)]
    last_row_id: Option<i64>,
    #[serde(default)]
    rows_read: Option<u64>,
    #[serde(default)]
    rows_written: Option<u64>,
    #[serde(default)]
    size_after: Option<u64>,
}

/// D1 API error
#[derive(Deserialize)]
struct D1Error {
    code: Option<i32>,
    message: String,
}

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

fn json_value_to_column_value(value: serde_json::Value) -> ColumnValue {
    match value {
        serde_json::Value::Null => ColumnValue::Null,
        serde_json::Value::Bool(b) => ColumnValue::Integer(if b { 1 } else { 0 }),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                ColumnValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                ColumnValue::Real(f)
            } else {
                ColumnValue::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => ColumnValue::Text(s),
        serde_json::Value::Array(arr) => ColumnValue::Blob(
            arr.iter()
                .filter_map(|v| v.as_u64().map(|n| n as u8))
                .collect(),
        ),
        serde_json::Value::Object(_) => ColumnValue::Text(value.to_string()),
    }
}

fn column_value_to_json(value: ColumnValue) -> serde_json::Value {
    match value {
        ColumnValue::Null => serde_json::Value::Null,
        ColumnValue::Integer(i) => serde_json::Value::Number(i.into()),
        ColumnValue::Real(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        ColumnValue::Text(s) => serde_json::Value::String(s),
        ColumnValue::Blob(b) => serde_json::Value::Array(
            b.into_iter()
                .map(|byte| serde_json::Value::Number(byte.into()))
                .collect(),
        ),
    }
}

// ---------------------------------------------------------------------------
// WIT types::Host — empty (all methods live on the resource)
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::cf_d1::types::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// WIT query::Host — empty (resource lives in HostD1Client)
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::cf_d1::query::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// WIT query::HostD1Client — resource constructor + methods
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::cf_d1::query::HostD1Client for ActiveCtx<'a> {
    async fn new(
        &mut self,
        config: Option<D1Config>,
    ) -> wasmtime::Result<Resource<D1ClientHandle>> {
        let Some(plugin) = self.get_plugin::<CloudflareD1>(PLUGIN_ID) else {
            return Err(wasmtime::Error::msg("cf-d1 plugin not available"));
        };

        let component_id: Arc<str> = self.component_id.clone();
        let lock = plugin.tracker.read().await;
        let Some(data) = lock.get_component_data(&component_id) else {
            return Err(wasmtime::Error::msg("component not tracked"));
        };

        let account_id = match resolve_field(
            config.as_ref().map(|c| c.account_id.clone()),
            &data.interface_config,
            "account_id",
        ) {
            Ok(v) => v,
            Err(_) => {
                return Err(wasmtime::Error::msg(
                    "missing account_id: provide via constructor or interface config",
                ));
            }
        };

        let api_token = match resolve_field(
            config.as_ref().map(|c| c.api_token.clone()),
            &data.interface_config,
            "api_token",
        ) {
            Ok(v) => v,
            Err(_) => {
                return Err(wasmtime::Error::msg(
                    "missing api_token: provide via constructor or interface config",
                ));
            }
        };

        let database_id = match resolve_field(
            config.as_ref().map(|c| c.database_id.clone()),
            &data.interface_config,
            "database_id",
        ) {
            Ok(v) => v,
            Err(_) => {
                return Err(wasmtime::Error::msg(
                    "missing database_id: provide via constructor or interface config",
                ));
            }
        };

        drop(lock);

        // Build HTTP client with auth
        let client = Client::builder()
            .default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert(
                    reqwest::header::AUTHORIZATION,
                    format!("Bearer {api_token}").parse()?,
                );
                headers.insert(reqwest::header::CONTENT_TYPE, "application/json".parse()?);
                headers
            })
            .build()
            .map_err(|e| wasmtime::Error::msg(format!("failed to create HTTP client: {e}")))?;

        debug!(
            account_id = %account_id,
            database_id = %database_id,
            "Created D1 client"
        );

        let handle = D1ClientHandle {
            account_id,
            database_id,
            client,
        };

        let resource = self.table.push(handle)?;
        Ok(resource)
    }

    async fn query(
        &mut self,
        handle: Resource<D1ClientHandle>,
        sql: String,
        params: Vec<ColumnValue>,
    ) -> wasmtime::Result<Result<QueryResult, QueryError>> {
        let Some(plugin) = self.get_plugin::<CloudflareD1>(PLUGIN_ID) else {
            return Ok(Err(QueryError::ConnectionError(
                "cf-d1 plugin not available".to_string(),
            )));
        };
        plugin.record_query("query");

        let h = self.table.get(&handle)?;
        let config_account_id = h.account_id.clone();
        let config_database_id = h.database_id.clone();
        let client = h.client.clone();

        debug!(
            database_id = %config_database_id,
            sql = %sql,
            params_count = params.len(),
            "Executing D1 query"
        );

        let url = format!(
            "https://api.cloudflare.com/client/v4/accounts/{config_account_id}/d1/database/{config_database_id}/query"
        );

        let json_params: Vec<serde_json::Value> =
            params.into_iter().map(column_value_to_json).collect();
        let request = D1QueryRequest {
            sql,
            params: if json_params.is_empty() {
                None
            } else {
                Some(json_params)
            },
        };

        let response = match client.post(&url).json(&request).send().await {
            Ok(r) => r,
            Err(e) => {
                return Ok(Err(QueryError::ConnectionError(format!(
                    "HTTP request failed: {e}"
                ))));
            }
        };
        let status_code = response.status();
        let content = match response.text().await {
            Ok(c) => c,
            Err(err) => {
                return Ok(Err(QueryError::Unexpected(format!(
                    "Read HTTP response failed: {err}"
                ))));
            }
        };
        debug!("d1 response status: {status_code}, content: {content}");

        let d1_response: D1Response = match serde_json::from_str(&content) {
            Ok(r) => r,
            Err(e) => {
                return Ok(Err(QueryError::Unexpected(format!(
                    "Failed to parse response: {e}"
                ))));
            }
        };

        if !d1_response.success {
            let error_msg = d1_response
                .errors
                .first()
                .map(|e| format!("{}: {}", e.code.unwrap_or(0), e.message))
                .unwrap_or_else(|| "Unknown D1 error".to_string());
            return Ok(Err(QueryError::DatabaseError(error_msg)));
        }

        let d1_result = d1_response.result.first();
        let (columns, rows, rows_affected, last_insert_rowid) = match d1_result {
            Some(result) => {
                let columns: Vec<ColumnMeta> = result
                    .results
                    .first()
                    .map(|row| {
                        row.keys()
                            .map(|k| ColumnMeta {
                                name: k.clone(),
                                column_type: None,
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let rows = result
                    .results
                    .iter()
                    .map(|row| {
                        let values = row
                            .values()
                            .map(|v| json_value_to_column_value(v.clone()))
                            .collect::<Vec<_>>();
                        ResultRow { values }
                    })
                    .collect();
                let rows_affected = result.meta.as_ref().and_then(|m| m.changes).unwrap_or(0);
                let last_insert_rowid = result.meta.as_ref().and_then(|m| m.last_row_id);
                (columns, rows, rows_affected, last_insert_rowid)
            }
            None => (vec![], vec![], 0, None),
        };

        Ok(Ok(QueryResult {
            columns,
            rows,
            rows_affected,
            last_insert_rowid,
        }))
    }

    async fn query_batch(
        &mut self,
        handle: Resource<D1ClientHandle>,
        sql: String,
    ) -> wasmtime::Result<Result<Vec<QueryResult>, QueryError>> {
        let Some(plugin) = self.get_plugin::<CloudflareD1>(PLUGIN_ID) else {
            return Ok(Err(QueryError::ConnectionError(
                "cf-d1 plugin not available".to_string(),
            )));
        };
        plugin.record_query("query_batch");

        let h = self.table.get(&handle)?;
        let config_account_id = h.account_id.clone();
        let config_database_id = h.database_id.clone();
        let client = h.client.clone();

        debug!(
            database_id = %config_database_id,
            sql_len = sql.len(),
            "Executing D1 batch query"
        );

        let url = format!(
            "https://api.cloudflare.com/client/v4/accounts/{config_account_id}/d1/database/{config_database_id}/raw"
        );
        let request = D1RawRequest { sql };

        let response = match client.post(&url).json(&request).send().await {
            Ok(r) => r,
            Err(e) => {
                return Ok(Err(QueryError::ConnectionError(format!(
                    "HTTP request failed: {e}"
                ))));
            }
        };
        let status_code = response.status();
        let content = match response.text().await {
            Ok(c) => c,
            Err(err) => {
                return Ok(Err(QueryError::Unexpected(format!(
                    "Read HTTP response failed: {err}"
                ))));
            }
        };
        debug!("d1 response status: {status_code}, content: {content}");

        let d1_response: D1ResponseRaw = match serde_json::from_str(&content) {
            Ok(r) => r,
            Err(e) => {
                return Ok(Err(QueryError::Unexpected(format!(
                    "Failed to parse response: {e}"
                ))));
            }
        };

        if !d1_response.success {
            let error_msg = d1_response
                .errors
                .first()
                .map(|e| format!("{}: {}", e.code.unwrap_or(0), e.message))
                .unwrap_or_else(|| "Unknown D1 error".to_string());
            return Ok(Err(QueryError::DatabaseError(error_msg)));
        }

        let results: Vec<QueryResult> = d1_response
            .result
            .iter()
            .map(|result| {
                let columns: Vec<ColumnMeta> = result
                    .results
                    .columns
                    .iter()
                    .map(|k| ColumnMeta {
                        name: k.clone(),
                        column_type: None,
                    })
                    .collect();
                let rows: Vec<ResultRow> = result
                    .results
                    .rows
                    .iter()
                    .map(|row| {
                        let values = row
                            .iter()
                            .map(|v| json_value_to_column_value(v.clone()))
                            .collect();
                        ResultRow { values }
                    })
                    .collect();
                let rows_affected = result.meta.as_ref().and_then(|m| m.changes).unwrap_or(0);
                let last_insert_rowid = result.meta.as_ref().and_then(|m| m.last_row_id);
                QueryResult {
                    columns,
                    rows,
                    rows_affected,
                    last_insert_rowid,
                }
            })
            .collect();

        Ok(Ok(results))
    }

    async fn query_one(
        &mut self,
        handle: Resource<D1ClientHandle>,
        sql: String,
        params: Vec<ColumnValue>,
    ) -> wasmtime::Result<Result<Option<ResultRow>, QueryError>> {
        let result = self.query(handle, sql, params).await?;
        match result {
            Ok(query_result) => Ok(Ok(query_result.rows.into_iter().next())),
            Err(e) => Ok(Err(e)),
        }
    }

    async fn stop(
        &mut self,
        _handle: Resource<D1ClientHandle>,
    ) -> wasmtime::Result<Result<(), QueryError>> {
        // No-op: D1 has no background tasks
        Ok(Ok(()))
    }

    async fn drop(&mut self, rep: Resource<D1ClientHandle>) -> wasmtime::Result<()> {
        // Just remove from the table, no cleanup needed
        let _ = self.table.delete(rep);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// HostPlugin implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl HostPlugin for CloudflareD1 {
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: HashSet::from([WitInterface::from("custom:cf-d1/query@0.1.0")]),
            ..Default::default()
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        let Some(interface) = interfaces
            .iter()
            .find(|i| i.namespace == "custom" && i.package == "cf-d1")
        else {
            tracing::warn!(
                "CloudflareD1 plugin requested for non-cf:d1 interface(s): {:?}",
                interfaces
            );
            return Ok(());
        };

        let interface_config = interface.config.clone();

        bindings::custom::cf_d1::types::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;
        bindings::custom::cf_d1::query::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;

        let WorkloadItem::Component(component_handle) = item else {
            return Ok(());
        };

        debug!(
            component_id = component_handle.id(),
            "CloudflareD1 plugin bound to component"
        );

        self.tracker
            .write()
            .await
            .add_component(component_handle, ComponentData { interface_config });

        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        self.tracker
            .write()
            .await
            .remove_workload_with_cleanup(workload_id, |_| async {}, |_| async {})
            .await;
        debug!(workload_id = %workload_id, "CloudflareD1 plugin unbound");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_id() {
        let plugin = CloudflareD1::new();
        assert_eq!(plugin.id(), PLUGIN_ID);
    }

    #[test]
    fn test_world_imports() {
        let plugin = CloudflareD1::new();
        let world = plugin.world();
        assert!(
            world
                .exports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "cf-d1")
        );
    }

    #[test]
    fn test_default() {
        let plugin = CloudflareD1::default();
        assert_eq!(plugin.id(), PLUGIN_ID);
    }

    #[test]
    fn test_json_value_to_column_value() {
        assert!(matches!(
            json_value_to_column_value(serde_json::Value::Null),
            ColumnValue::Null
        ));
        assert!(matches!(
            json_value_to_column_value(serde_json::Value::Bool(true)),
            ColumnValue::Integer(1)
        ));
        assert!(matches!(
            json_value_to_column_value(serde_json::json!(42)),
            ColumnValue::Integer(42)
        ));
        assert!(matches!(
            json_value_to_column_value(serde_json::json!(3.14)),
            ColumnValue::Real(_)
        ));
        assert!(matches!(
            json_value_to_column_value(serde_json::json!("hello")),
            ColumnValue::Text(s) if s == "hello"
        ));
    }

    #[test]
    fn test_column_value_to_json() {
        assert_eq!(
            column_value_to_json(ColumnValue::Null),
            serde_json::Value::Null
        );
        assert_eq!(
            column_value_to_json(ColumnValue::Integer(42)),
            serde_json::json!(42)
        );
        assert_eq!(
            column_value_to_json(ColumnValue::Text("hello".to_string())),
            serde_json::json!("hello")
        );
    }
}
