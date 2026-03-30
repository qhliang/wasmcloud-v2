//! # Cloudflare D1 Host Plugin
//!
//! This module implements a wasmCloud host plugin that provides `cf:d1@0.1.0`
//! interfaces using Cloudflare D1 as the backend SQL database.
//!
//! ## Usage
//!
//! ```ignore
//! use custom_plugin_cf_d1::CloudflareD1;
//! use wash_runtime::host::HostBuilder;
//! use std::sync::Arc;
//!
//! // Create the plugin (credentials are configured per-workload via interface config)
//! let cf_d1 = CloudflareD1::new();
//!
//! // Add to host builder
//! let host = HostBuilder::new()
//!     .with_plugin(Arc::new(cf_d1))?
//!     .build()?;
//! ```
//!
//! ## per-Workload Configuration
//!
//! Each workload must configure its credentials via interface config:
//!
//! ```ignore
//! // In the workload manifest or interface configuration:
//! // cf:d1:
//! //   config:
//! //     account_id: "your-cloudflare-account-id"
//! //     api_token: "your-cloudflare-api-token"
//! //     database_id: "your-d1-database-id"
//! ```
//!
//! ## API Endpoints
//!
//! Cloudflare D1 has two endpoints with different response formats:
//!
//! ### /query endpoint
//! - URL: https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query
//! - Response: `{"result":[{"results":[{"id":1,"name":"Test"},...}],"success":true,...}]}`
//! - The `results` field is an **array of row objects** (key-value pairs)
//!
//! ### /raw endpoint
//! - URL: https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/raw
//! - Response: `{"result":[{"results":{"columns":["id","name",...],"rows":[[1,"Test"],...]},"success":true,...}]}`
//! - The `results` field is an **object** with `columns` and `rows` arrays

//!

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use indexmap::IndexMap;
use opentelemetry::metrics::Counter;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::debug;

use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::WorkloadItem;
use wash_runtime::plugin::HostPlugin;
use wash_runtime::wit::{WitInterface, WitWorld};

mod bindings {
    wasmtime::component::bindgen!({
        world: "d1",
        imports: {
            default: async | trappable | tracing,
        },
    });
}

use bindings::custom::cf_d1::types::{ColumnMeta, ColumnValue, QueryError, QueryResult, ResultRow};

const PLUGIN_ID: &str = "cf-d1";

/// Configuration for Cloudflare D1 (per-workload)
#[derive(Clone, Debug)]
pub struct CloudflareD1Config {
    /// Cloudflare account ID
    pub account_id: String,
    /// Cloudflare API token with D1 permissions
    pub api_token: String,
    /// D1 database ID
    pub database_id: String,
}

/// Cloudflare D1 SQL database plugin
#[derive(Clone, Default)]
pub struct CloudflareD1 {
    /// Mapping from workload_id to workload-specific config
    configs: Arc<RwLock<HashMap<String, CloudflareD1Config>>>,
    /// HTTP clients per workload
    clients: Arc<RwLock<HashMap<String, Client>>>,
    /// Metrics
    metrics: Arc<CloudflareD1Metrics>,
}

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
impl CloudflareD1 {
    /// Create a new Cloudflare D1 plugin
    /// Credentials are configured per-workload via interface config
    pub fn new() -> Self {
        let metrics = CloudflareD1Metrics::new();
        Self {
            configs: Arc::new(RwLock::new(HashMap::new())),
            clients: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(metrics),
        }
    }
    fn record_query(&self, operation: &str) {
        let attributes = [opentelemetry::KeyValue::new(
            "operation",
            operation.to_string(),
        )];
        self.metrics.queries_total.add(1, &attributes);
    }
    async fn get_or_create_client(&self, workload_id: &str) -> anyhow::Result<Client> {
        // Check if client already exists
        {
            let clients = self.clients.read().await;
            if let Some(client) = clients.get(workload_id) {
                return Ok(client.clone());
            }
        }
        // Get config for this workload
        let configs = self.configs.read().await;
        let config = configs
            .get(workload_id)
            .ok_or_else(|| anyhow::anyhow!("No D1 config found for workload '{}'", workload_id))?;
        let api_token = config.api_token.clone();
        drop(configs);
        // Build HTTP client with auth
        let client = Client::builder()
            .default_headers({
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert(
                    reqwest::header::AUTHORIZATION,
                    format!("Bearer {}", api_token).parse()?,
                );
                headers.insert(reqwest::header::CONTENT_TYPE, "application/json".parse()?);
                headers
            })
            .build()?;
        // Cache the client
        {
            let mut clients = self.clients.write().await;
            clients.insert(workload_id.to_string(), client.clone());
        }
        Ok(client)
    }
}
// ============================================================================
// D1 API Request/Response Types
// ============================================================================
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
// ============================================================================
// Query Interface Implementation
// ============================================================================
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
        serde_json::Value::Array(arr) => {
            // Treat as blob if it looks like bytes
            ColumnValue::Blob(
                arr.iter()
                    .filter_map(|v| v.as_u64().map(|n| n as u8))
                    .collect(),
            )
        }
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
impl<'a> bindings::custom::cf_d1::query::Host for ActiveCtx<'a> {
    async fn query(
        &mut self,
        sql: String,
        params: Vec<ColumnValue>,
    ) -> wasmtime::Result<Result<QueryResult, QueryError>> {
        let Some(plugin) = self.get_plugin::<CloudflareD1>(PLUGIN_ID) else {
            return Ok(Err(QueryError::ConnectionError(
                "Cloudflare D1 plugin not available".to_string(),
            )));
        };
        plugin.record_query("query");
        let workload_id = self.workload_id.as_ref().to_string();
        // Get config
        let configs = plugin.configs.read().await;
        let config = match configs.get(&workload_id) {
            Some(cfg) => cfg.clone(),
            None => {
                return Ok(Err(QueryError::ConnectionError(format!(
                    "No D1 config found for workload '{}'",
                    workload_id
                ))));
            }
        };
        drop(configs);
        debug!(
            workload_id = %workload_id,
            database_id = %config.database_id,
            sql = %sql,
            params_count = params.len(),
            "Executing D1 query"
        );
        // Get or create HTTP client
        let client = match plugin.get_or_create_client(&workload_id).await {
            Ok(c) => c,
            Err(e) => {
                return Ok(Err(QueryError::ConnectionError(format!(
                    "Failed to create HTTP client: {}",
                    e
                ))));
            }
        };
        // Build API URL
        let url = format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/d1/database/{}/query",
            config.account_id, config.database_id
        );
        // Convert params to JSON
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
        // Execute query
        let response = match client.post(&url).json(&request).send().await {
            Ok(r) => r,
            Err(e) => {
                return Ok(Err(QueryError::ConnectionError(format!(
                    "HTTP request failed: {}",
                    e
                ))));
            }
        };
        let status_code = response.status();
        let content = match response.text().await {
            Ok(c) => c,
            Err(err) => {
                return Ok(Err(QueryError::Unexpected(format!(
                    "Read HTTP response failed: {}",
                    err
                ))));
            }
        };
        debug!("d1 response status: {status_code}, content: {content}");
        // Parse response
        let d1_response: D1Response = match serde_json::from_str(&content) {
            Ok(r) => r,
            Err(e) => {
                return Ok(Err(QueryError::Unexpected(format!(
                    "Failed to parse response: {}",
                    e
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
        // Convert D1 result to our QueryResult
        let d1_result = d1_response.result.first();
        let (columns, rows, rows_affected, last_insert_rowid) = match d1_result {
            Some(result) => {
                // Extract column names from first row (query endpoint returns array of objects)
                let columns: Vec<ColumnMeta> = result
                    .results
                    .first()
                    .map(|row| {
                        row.keys()
                            .map(|k| ColumnMeta {
                                name: k.clone(),
                                column_type: None, // D1 doesn't provide type info in results
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                // Convert rows (each row is a HashMap)
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
        sql: String,
    ) -> wasmtime::Result<Result<Vec<QueryResult>, QueryError>> {
        let Some(plugin) = self.get_plugin::<CloudflareD1>(PLUGIN_ID) else {
            return Ok(Err(QueryError::ConnectionError(
                "Cloudflare D1 plugin not available".to_string(),
            )));
        };
        plugin.record_query("query_batch");
        let workload_id = self.workload_id.as_ref().to_string();
        // Get config
        let configs = plugin.configs.read().await;
        let config = match configs.get(&workload_id) {
            Some(cfg) => cfg.clone(),
            None => {
                return Ok(Err(QueryError::ConnectionError(format!(
                    "No D1 config found for workload '{}'",
                    workload_id
                ))));
            }
        };
        drop(configs);
        debug!(
            workload_id = %workload_id,
            database_id = %config.database_id,
            sql_len = sql.len(),
            "Executing D1 batch query"
        );
        // Get or create HTTP client
        let client = match plugin.get_or_create_client(&workload_id).await {
            Ok(c) => c,
            Err(e) => {
                return Ok(Err(QueryError::ConnectionError(format!(
                    "Failed to create HTTP client: {}",
                    e
                ))));
            }
        };
        // Build API URL for raw endpoint
        let url = format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/d1/database/{}/raw",
            config.account_id, config.database_id
        );
        let request = D1RawRequest { sql };
        // Execute query
        let response = match client.post(&url).json(&request).send().await {
            Ok(r) => r,
            Err(e) => {
                return Ok(Err(QueryError::ConnectionError(format!(
                    "HTTP request failed: {}",
                    e
                ))));
            }
        };
        let status_code = response.status();
        let content = match response.text().await {
            Ok(c) => c,
            Err(err) => {
                return Ok(Err(QueryError::Unexpected(format!(
                    "Read HTTP response failed: {}",
                    err
                ))));
            }
        };
        debug!("d1 response status: {status_code}, content: {content}");
        // Parse response
        let d1_response: D1ResponseRaw = match serde_json::from_str(&content) {
            Ok(r) => r,
            Err(e) => {
                return Ok(Err(QueryError::Unexpected(format!(
                    "Failed to parse response: {}",
                    e
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
        // Convert all D1 results
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
        sql: String,
        params: Vec<ColumnValue>,
    ) -> wasmtime::Result<Result<Option<ResultRow>, QueryError>> {
        // Execute normal query and return first row
        let result = self.query(sql, params).await?;
        match result {
            Ok(query_result) => Ok(Ok(query_result.rows.into_iter().next())),
            Err(e) => Ok(Err(e)),
        }
    }
}
#[async_trait]
impl HostPlugin for CloudflareD1 {
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }
    fn world(&self) -> WitWorld {
        WitWorld {
            imports: HashSet::from([WitInterface::from("custom:cf-d1/query@0.1.0")]),
            ..Default::default()
        }
    }
    async fn on_workload_item_bind<'a>(
        &self,
        component_handle: &mut WorkloadItem<'a>,
        interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        // Find the cf:d1 interface
        let d1_interface = interfaces
            .iter()
            .find(|i| i.namespace == "custom" && i.package == "cf-d1");
        let Some(interface) = d1_interface else {
            tracing::warn!(
                "CloudflareD1 plugin requested for non-cf:d1 interface(s): {:?}",
                interfaces
            );
            return Ok(());
        };
        let workload_id = component_handle.workload_id().to_string();
        // Extract config from interface
        let account_id = interface
            .config
            .get("account_id")
            .cloned()
            .unwrap_or_else(|| {
                tracing::error!(
                    workload_id = %workload_id,
                    "No 'account_id' configured for cf:d1 interface"
                );
                String::new()
            });
        let api_token = interface
            .config
            .get("api_token")
            .cloned()
            .unwrap_or_else(|| {
                tracing::error!(
                    workload_id = %workload_id,
                    "No 'api_token' configured for cf:d1 endpoint"
                );
                String::new()
            });
        let database_id = interface
            .config
            .get("database_id")
            .cloned()
            .unwrap_or_else(|| {
                tracing::error!(
                    workload_id = %workload_id,
                    "No 'database_id' configured for cf:d1 interface"
                );
                String::new()
            });
        if account_id.is_empty() || api_token.is_empty() || database_id.is_empty() {
            tracing::error!(
                workload_id = %workload_id,
                "Cloudflare D1 plugin bound with incomplete config. \
                 Required: account_id, api_token, database_id"
            );
        }
        debug!(
            workload_id = %workload_id,
            account_id = %account_id,
            database_id = %database_id,
            "Configuring Cloudflare D1 for workload"
        );
        // Save the config for this workload
        {
            let mut configs = self.configs.write().await;
            configs.insert(
                workload_id.clone(),
                CloudflareD1Config {
                    account_id,
                    api_token,
                    database_id,
                },
            );
        }
        debug!(
            workload_id = %workload_id,
            "Adding Cloudflare D1 query interface to linker for workload"
        );
        let linker = component_handle.linker();
        bindings::custom::cf_d1::query::add_to_linker::<_, SharedCtx>(linker, extract_active_ctx)?;
        debug!("CloudflareD1 plugin bound to workload '{workload_id}'");
        Ok(())
    }
    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        // Clean up the config and client for this workload
        {
            let mut configs = self.configs.write().await;
            configs.remove(workload_id);
        }
        {
            let mut clients = self.clients.write().await;
            clients.remove(workload_id);
        }
        debug!("CloudflareD1 plugin unbound from workload '{workload_id}'");
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
                .imports
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
