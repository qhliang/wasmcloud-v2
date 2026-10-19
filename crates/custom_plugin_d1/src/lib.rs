//! # Structured Cloudflare D1 host plugin
//!
//! Table-data access to a single Cloudflare D1 database, with no way to reach
//! the schema.
//!
//! Two properties drive the design.
//!
//! **No SQL text crosses the boundary.** A component supplies the fields of a
//! request; [`sql`] turns those into parameterised SQL. Schema statements are
//! therefore unreachable by construction, not by filtering — Cloudflare D1
//! happily runs `CREATE TABLE`, `ALTER TABLE` and `DROP TABLE`, so a host-side
//! filter would be the only line of defence and would have to be airtight.
//!
//! **Configuration is the operator's alone.** The resource constructor takes
//! no arguments, so a component cannot redirect the client at another account
//! or database. Credentials come from the host interface config only.
//!
//! The transport, client caching and timeouts are those of
//! `custom_plugin_cf_d1`, which has them because they were added after two
//! production incidents; the comments below record why.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use indexmap::IndexMap;
use opentelemetry::metrics::Counter;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error};
use wasmtime::component::Resource;

use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::WorkloadItem;
use wash_runtime::plugin::config::resolve_field;
use wash_runtime::plugin::{HostPlugin, WitInterfaces, WorkloadTracker};
use wash_runtime::wit::{WitInterface, WitWorld};

mod bindings {
    wasmtime::component::bindgen!({
        world: "d1-table",
        imports: {
            default: async | trappable | tracing,
        },
        with: {
            "custom:d1-table/table.d1-table-client": super::D1TableClientHandle,
        },
    });
}

use bindings::custom::d1_table::types::{
    AggregateFn, AggregateProjection, ColumnMeta, ColumnRef, ColumnValue, ComparisonOp, Condition,
    Field, JoinClause, JoinKind, OrderBy, Page, Projection, QueryResult, ResultRow, SelectItem,
    SortDirection, TableError, UpsertMode, UpsertSpec,
};

pub mod sql;

use sql::{
    BuildError, ComparisonOp as SqlComparisonOp, ConditionOp, Expr, Field as SqlField, Join,
    JoinKind as SqlJoinKind, OrderBy as SqlOrderBy, SelectItem as SqlSelectItem, SelectSpec,
    Statement, UpsertMode as SqlUpsertMode, UpsertSpec as SqlUpsertSpec,
};

const PLUGIN_ID: &str = "d1-table";

/// Connect timeout for the D1 HTTP client.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Whole-request timeout for the D1 HTTP client.
///
/// Both bounds exist because of a production incident: with no timeout at all, a
/// network wobble let requests hang until the TCP/TLS layer gave up — 40 to 52
/// seconds in the observed logs — and that whole time the host's *serial*
/// callback dispatcher was blocked behind them. A fast failure lets the caller
/// retry instead of queueing.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// HTTP client cache key: `account_id|database_id|<hash of token>`.
///
/// Only the hash is kept, so no extra copy of the token sits in memory, while a
/// token rotation still misses naturally and rebuilds the client.
fn client_cache_key(account_id: &str, database_id: &str, api_token: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    api_token.hash(&mut hasher);
    format!("{account_id}|{database_id}|{:016x}", hasher.finish())
}

// ---------------------------------------------------------------------------
// Host state
// ---------------------------------------------------------------------------

/// Host-side state for one `d1-table-client` resource.
pub struct D1TableClientHandle {
    account_id: String,
    database_id: String,
    client: Client,
}

/// Per-component data captured at bind time.
struct ComponentData {
    /// Host operator configuration for this interface. The only source of
    /// credentials — the constructor takes nothing to override them with.
    interface_config: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

struct D1TableMetrics {
    queries_total: Counter<u64>,
}

impl Default for D1TableMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl D1TableMetrics {
    fn new() -> Self {
        let meter = opentelemetry::global::meter("d1-table");
        let queries_total = meter
            .u64_counter("d1_table_queries_total")
            .with_description("Total number of statements executed on Cloudflare D1")
            .build();
        Self { queries_total }
    }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/// Cloudflare D1 table-data plugin.
#[derive(Clone)]
pub struct D1Table {
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
    metrics: Arc<D1TableMetrics>,
    /// HTTP clients keyed by `account_id|database_id|token_hash`.
    ///
    /// **Why this must be cached**: the host may re-instantiate a guest on every
    /// exported call, which resets the guest's own `OnceLock`, so every
    /// constructor body runs again. Uncached, that means a fresh
    /// `reqwest::Client` per query — a fresh TLS handshake per query — so on a
    /// network wobble each query separately ran to timeout and latency was
    /// inflated for nothing (2026-09-11 production investigation: every query
    /// logged `Created D1 client` + `starting new connection`). `reqwest::Client`
    /// pools connections internally and clones cheaply, so caching means one
    /// pool per (account, database, token).
    clients: Arc<Mutex<HashMap<String, Client>>>,
}

impl Default for D1Table {
    fn default() -> Self {
        Self::new()
    }
}

impl D1Table {
    /// Create a new D1 table-data plugin.
    pub fn new() -> Self {
        Self {
            tracker: Arc::new(RwLock::new(WorkloadTracker::default())),
            metrics: Arc::new(D1TableMetrics::new()),
            clients: Arc::new(Mutex::new(HashMap::new())),
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
// Cloudflare D1 wire types
// ---------------------------------------------------------------------------

/// Body of a `POST .../query` request.
#[derive(Serialize)]
struct D1QueryRequest {
    sql: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<Vec<serde_json::Value>>,
}

/// Response of `POST .../query`.
#[derive(Deserialize)]
struct D1Response {
    result: Vec<D1Result>,
    success: bool,
    #[serde(default)]
    errors: Vec<D1Error>,
}

/// One statement's result.
#[derive(Deserialize, Default)]
struct D1Result {
    /// Rows as objects, keyed by column name. Column order follows the
    /// projection, which is the order this plugin always asks for.
    #[serde(default)]
    results: Vec<IndexMap<String, serde_json::Value>>,
    #[serde(default)]
    meta: Option<D1Meta>,
}

/// Result metadata.
#[derive(Deserialize, Default)]
struct D1Meta {
    #[serde(default)]
    changes: Option<u64>,
    #[serde(default)]
    last_row_id: Option<i64>,
}

/// One error from the Cloudflare envelope.
#[derive(Deserialize)]
struct D1Error {
    code: Option<i32>,
    message: String,
}

// ---------------------------------------------------------------------------
// Value conversion
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
// WIT -> sql module
// ---------------------------------------------------------------------------

/// Map a build failure onto the WIT error a component receives.
///
/// `Identifier` and `Request` both become recoverable `result::Err` values: the
/// caller wrote the request wrongly and can fix it. Only infrastructure faults
/// trap, so a component can tell "my request was wrong" from "the service is
/// unavailable" and choose to retry only the latter.
fn table_error(err: BuildError) -> TableError {
    match err {
        BuildError::Identifier(detail) => TableError::InvalidIdentifier(format!(
            "{detail} (must match {})",
            sql::IDENTIFIER_PATTERN
        )),
        BuildError::Request(detail) => TableError::InvalidRequest(detail),
    }
}

fn convert_comparison(op: ComparisonOp) -> SqlComparisonOp {
    match op {
        ComparisonOp::Eq => SqlComparisonOp::Eq,
        ComparisonOp::Ne => SqlComparisonOp::Ne,
        ComparisonOp::Lt => SqlComparisonOp::Lt,
        ComparisonOp::Le => SqlComparisonOp::Le,
        ComparisonOp::Gt => SqlComparisonOp::Gt,
        ComparisonOp::Ge => SqlComparisonOp::Ge,
        ComparisonOp::Like => SqlComparisonOp::Like,
        ComparisonOp::IsNull => SqlComparisonOp::IsNull,
        ComparisonOp::NotNull => SqlComparisonOp::NotNull,
    }
}

fn convert_condition(condition: Condition) -> ConditionOp {
    ConditionOp {
        column: condition.column,
        op: convert_comparison(condition.op),
        value: condition.value.map(column_value_to_json),
    }
}

fn convert_conditions(conditions: Vec<Condition>) -> Vec<ConditionOp> {
    conditions.into_iter().map(convert_condition).collect()
}

fn convert_join(join: JoinClause) -> Join {
    Join {
        kind: match join.kind {
            JoinKind::Inner => SqlJoinKind::Inner,
            JoinKind::Left => SqlJoinKind::Left,
        },
        table: join.table,
        on: convert_conditions(join.on),
    }
}

fn convert_aggregate(func: AggregateFn) -> sql::AggregateFn {
    match func {
        AggregateFn::Count => sql::AggregateFn::Count,
        AggregateFn::Sum => sql::AggregateFn::Sum,
        AggregateFn::Avg => sql::AggregateFn::Avg,
        AggregateFn::Min => sql::AggregateFn::Min,
        AggregateFn::Max => sql::AggregateFn::Max,
    }
}

fn convert_column_ref(reference: ColumnRef) -> (Option<String>, String) {
    (reference.table, reference.name)
}

fn convert_order_by(order: OrderBy) -> SqlOrderBy {
    SqlOrderBy {
        column: order.column,
        descending: order.direction == SortDirection::Desc,
    }
}

fn convert_fields(fields: Vec<Field>) -> Vec<SqlField> {
    fields
        .into_iter()
        .map(|f| SqlField {
            name: f.name,
            value: column_value_to_json(f.value),
        })
        .collect()
}

fn convert_page(page: Page) -> (Option<u32>, Option<u32>) {
    (page.limit, page.offset)
}

// ---------------------------------------------------------------------------
// WIT types::Host — empty (every method lives on the resource)
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::d1_table::types::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// WIT table::Host — empty (the resource lives in HostD1TableClient)
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::d1_table::table::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// WIT table::HostD1TableClient
// ---------------------------------------------------------------------------

/// Run one built statement against D1 and shape the response.
///
/// Shared by every operation: by the time this is called the SQL exists and
/// carries no schema statement, so the remaining concerns are transport,
/// authentication and decoding.
async fn execute(
    handle: &D1TableClientHandle,
    operation: &'static str,
    statement: Statement,
) -> wasmtime::Result<Result<QueryResult, TableError>> {
    let url = format!(
        "https://api.cloudflare.com/client/v4/accounts/{}/d1/database/{}/query",
        handle.account_id, handle.database_id
    );

    debug!(
        database_id = %handle.database_id,
        operation,
        sql = %statement.sql,
        params_count = statement.params.len(),
        "Executing D1 statement"
    );

    let request = D1QueryRequest {
        sql: statement.sql,
        params: if statement.params.is_empty() {
            None
        } else {
            Some(statement.params)
        },
    };

    let response = match handle.client.post(&url).json(&request).send().await {
        Ok(r) => r,
        Err(e) => {
            error!(
                database_id = %handle.database_id,
                operation,
                error = %e,
                "d1-table HTTP request failed"
            );
            return Ok(Err(TableError::ConnectionError(format!(
                "HTTP request failed: {e}"
            ))));
        }
    };
    let status_code = response.status();
    let content = match response.text().await {
        Ok(c) => c,
        Err(err) => {
            error!(
                database_id = %handle.database_id,
                operation,
                error = %err,
                "d1-table read response body failed"
            );
            return Ok(Err(TableError::Unexpected(format!(
                "read HTTP response failed: {err}"
            ))));
        }
    };
    if !status_code.is_success() {
        error!(
            database_id = %handle.database_id,
            operation,
            status = %status_code,
            body = %content,
            "d1-table non-200 HTTP status"
        );
        return Ok(Err(TableError::ConnectionError(format!(
            "HTTP {status_code}: {content}"
        ))));
    }

    let parsed: D1Response = match serde_json::from_str(&content) {
        Ok(r) => r,
        Err(e) => {
            error!(
                database_id = %handle.database_id,
                operation,
                error = %e,
                "d1-table failed to parse response"
            );
            return Ok(Err(TableError::Unexpected(format!(
                "failed to parse response: {e}"
            ))));
        }
    };

    if !parsed.success {
        let message = parsed
            .errors
            .first()
            .map(|e| format!("{}: {}", e.code.unwrap_or(0), e.message))
            .unwrap_or_else(|| "unknown D1 error".to_string());
        error!(
            database_id = %handle.database_id,
            operation,
            message = %message,
            "d1-table statement rejected by D1"
        );
        return Ok(Err(TableError::DatabaseError(message)));
    }

    // A write returns no rows, so an absent result is normal rather than an error.
    let result = parsed.result.first();
    let Some(result) = result else {
        return Ok(Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: 0,
            last_insert_rowid: None,
        }));
    };

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

    let rows: Vec<ResultRow> = result
        .results
        .iter()
        .map(|row| ResultRow {
            values: row
                .values()
                .map(|v| json_value_to_column_value(v.clone()))
                .collect(),
        })
        .collect();

    let rows_affected = result.meta.as_ref().and_then(|m| m.changes).unwrap_or(0);
    let last_insert_rowid = result.meta.as_ref().and_then(|m| m.last_row_id);

    Ok(Ok(QueryResult {
        columns,
        rows,
        rows_affected,
        last_insert_rowid,
    }))
}

/// The first value of the first row, as a `u64`.
///
/// `count` always projects `COUNT(*)`, so a row that is not a count is a
/// protocol violation rather than something a component could observe.
fn first_count(result: &QueryResult) -> Result<u64, TableError> {
    let value = result
        .rows
        .first()
        .and_then(|row| row.values.first())
        .ok_or_else(|| TableError::Unexpected("count returned no value".to_string()))?;

    match value {
        ColumnValue::Integer(n) => u64::try_from(*n)
            .map_err(|_| TableError::Unexpected(format!("count returned a negative value: {n}"))),
        other => Err(TableError::Unexpected(format!(
            "count returned a non-integer value: {other:?}"
        ))),
    }
}

impl<'a> bindings::custom::d1_table::table::HostD1TableClient for ActiveCtx<'a> {
    async fn new(&mut self) -> wasmtime::Result<Resource<D1TableClientHandle>> {
        let Ok(plugin) = self.try_get_plugin::<D1Table>(PLUGIN_ID) else {
            return Err(wasmtime::Error::msg("d1-table plugin not available"));
        };

        let component_id: Arc<str> = self.component_id.clone();
        let lock = plugin.tracker.read().await;
        let Some(data) = lock.get_component_data(&component_id) else {
            return Err(wasmtime::Error::msg("component not tracked"));
        };

        // `None` as the dynamic half is the whole point of this plugin's
        // constructor: there is nothing a component could pass, so `resolve_field`
        // can only find the operator's value.
        let account_id = resolve_field(None, &data.interface_config, "account_id")
            .map_err(|_| wasmtime::Error::msg("missing account_id in interface config"))?;
        let api_token = resolve_field(None, &data.interface_config, "api_token")
            .map_err(|_| wasmtime::Error::msg("missing api_token in interface config"))?;
        let database_id = resolve_field(None, &data.interface_config, "database_id")
            .map_err(|_| wasmtime::Error::msg("missing database_id in interface config"))?;

        drop(lock);

        let cache_key = client_cache_key(&account_id, &database_id, &api_token);
        let client = {
            let mut cache = plugin.clients.lock().await;
            match cache.get(&cache_key) {
                Some(client) => client.clone(),
                None => {
                    let client = Client::builder()
                        .connect_timeout(CONNECT_TIMEOUT)
                        .timeout(REQUEST_TIMEOUT)
                        .default_headers({
                            let mut headers = reqwest::header::HeaderMap::new();
                            headers.insert(
                                reqwest::header::AUTHORIZATION,
                                format!("Bearer {api_token}").parse()?,
                            );
                            headers
                                .insert(reqwest::header::CONTENT_TYPE, "application/json".parse()?);
                            headers
                        })
                        .build()
                        .map_err(|e| {
                            wasmtime::Error::msg(format!("failed to create HTTP client: {e}"))
                        })?;
                    cache.insert(cache_key, client.clone());
                    client
                }
            }
        };

        debug!(
            account_id = %account_id,
            database_id = %database_id,
            "D1 table client ready"
        );

        let resource = self.table.push(D1TableClientHandle {
            account_id,
            database_id,
            client,
        })?;
        Ok(resource)
    }

    async fn select(
        &mut self,
        handle: Resource<D1TableClientHandle>,
        table: String,
        joins: Vec<JoinClause>,
        projection: Vec<SelectItem>,
        conditions: Vec<Condition>,
        group_by: Vec<String>,
        order_by: Vec<OrderBy>,
        page: Page,
    ) -> wasmtime::Result<Result<QueryResult, TableError>> {
        let Ok(plugin) = self.try_get_plugin::<D1Table>(PLUGIN_ID) else {
            return Ok(Err(TableError::ConnectionError(
                "d1-table plugin not available".to_string(),
            )));
        };
        plugin.record_query("select");

        // The alias belongs to the item, not the expression, so it is threaded
        // through the conversion rather than dropped.
        let projection: Vec<SqlSelectItem> = projection
            .into_iter()
            .map(|item| {
                let expr = match item.expr {
                    Projection::Column(reference) => {
                        let (table, name) = convert_column_ref(reference);
                        Expr::Column { table, name }
                    }
                    Projection::CountAll => Expr::CountAll,
                    Projection::Aggregate(AggregateProjection { aggregate, column }) => {
                        let (table, name) = convert_column_ref(column);
                        Expr::Aggregate {
                            func: convert_aggregate(aggregate),
                            table,
                            name,
                        }
                    }
                };
                SqlSelectItem {
                    expr,
                    alias: item.alias,
                }
            })
            .collect();

        let (limit, offset) = convert_page(page);
        let statement = sql::build_select(&SelectSpec {
            table,
            joins: joins.into_iter().map(convert_join).collect(),
            projection,
            conditions: convert_conditions(conditions),
            group_by,
            order_by: order_by.into_iter().map(convert_order_by).collect(),
            limit,
            offset,
        });

        let statement = match statement {
            Ok(s) => s,
            Err(e) => return Ok(Err(table_error(e))),
        };

        let h = self.table.get(&handle)?;
        execute(h, "select", statement).await
    }

    async fn count(
        &mut self,
        handle: Resource<D1TableClientHandle>,
        table: String,
        joins: Vec<JoinClause>,
        conditions: Vec<Condition>,
    ) -> wasmtime::Result<Result<u64, TableError>> {
        let Ok(plugin) = self.try_get_plugin::<D1Table>(PLUGIN_ID) else {
            return Ok(Err(TableError::ConnectionError(
                "d1-table plugin not available".to_string(),
            )));
        };
        plugin.record_query("count");

        let statement = match sql::build_count(
            &table,
            joins.into_iter().map(convert_join).collect(),
            convert_conditions(conditions),
        ) {
            Ok(s) => s,
            Err(e) => return Ok(Err(table_error(e))),
        };

        let h = self.table.get(&handle)?;
        match execute(h, "count", statement).await? {
            Ok(result) => Ok(first_count(&result)),
            Err(e) => Ok(Err(e)),
        }
    }

    async fn insert(
        &mut self,
        handle: Resource<D1TableClientHandle>,
        table: String,
        fields: Vec<Field>,
    ) -> wasmtime::Result<Result<QueryResult, TableError>> {
        let Ok(plugin) = self.try_get_plugin::<D1Table>(PLUGIN_ID) else {
            return Ok(Err(TableError::ConnectionError(
                "d1-table plugin not available".to_string(),
            )));
        };
        plugin.record_query("insert");

        let statement = match sql::build_insert(&table, &convert_fields(fields)) {
            Ok(s) => s,
            Err(e) => return Ok(Err(table_error(e))),
        };

        let h = self.table.get(&handle)?;
        execute(h, "insert", statement).await
    }

    async fn upsert(
        &mut self,
        handle: Resource<D1TableClientHandle>,
        table: String,
        fields: Vec<Field>,
        spec: UpsertSpec,
    ) -> wasmtime::Result<Result<QueryResult, TableError>> {
        let Ok(plugin) = self.try_get_plugin::<D1Table>(PLUGIN_ID) else {
            return Ok(Err(TableError::ConnectionError(
                "d1-table plugin not available".to_string(),
            )));
        };
        plugin.record_query("upsert");

        let converted = SqlUpsertSpec {
            mode: match spec.mode {
                UpsertMode::Update => SqlUpsertMode::Update,
                UpsertMode::Ignore => SqlUpsertMode::Ignore,
            },
            conflict_columns: spec.conflict_columns,
            update_columns: spec.update_columns,
        };

        let statement = match sql::build_upsert(&table, &convert_fields(fields), &converted) {
            Ok(s) => s,
            Err(e) => return Ok(Err(table_error(e))),
        };

        let h = self.table.get(&handle)?;
        execute(h, "upsert", statement).await
    }

    async fn update(
        &mut self,
        handle: Resource<D1TableClientHandle>,
        table: String,
        fields: Vec<Field>,
        conditions: Vec<Condition>,
    ) -> wasmtime::Result<Result<QueryResult, TableError>> {
        let Ok(plugin) = self.try_get_plugin::<D1Table>(PLUGIN_ID) else {
            return Ok(Err(TableError::ConnectionError(
                "d1-table plugin not available".to_string(),
            )));
        };
        plugin.record_query("update");

        let converted = convert_conditions(conditions);
        let statement = match sql::build_update(&table, &convert_fields(fields), &converted) {
            Ok(s) => s,
            Err(e) => return Ok(Err(table_error(e))),
        };

        let h = self.table.get(&handle)?;
        execute(h, "update", statement).await
    }

    async fn delete(
        &mut self,
        handle: Resource<D1TableClientHandle>,
        table: String,
        conditions: Vec<Condition>,
    ) -> wasmtime::Result<Result<QueryResult, TableError>> {
        let Ok(plugin) = self.try_get_plugin::<D1Table>(PLUGIN_ID) else {
            return Ok(Err(TableError::ConnectionError(
                "d1-table plugin not available".to_string(),
            )));
        };
        plugin.record_query("delete");

        let converted = convert_conditions(conditions);
        let statement = match sql::build_delete(&table, &converted) {
            Ok(s) => s,
            Err(e) => return Ok(Err(table_error(e))),
        };

        let h = self.table.get(&handle)?;
        execute(h, "delete", statement).await
    }

    async fn stop(
        &mut self,
        _handle: Resource<D1TableClientHandle>,
    ) -> wasmtime::Result<Result<(), TableError>> {
        // D1 runs no background work, so there is nothing to tear down.
        Ok(Ok(()))
    }

    async fn drop(&mut self, rep: Resource<D1TableClientHandle>) -> wasmtime::Result<()> {
        let _ = self.table.delete(rep);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// HostPlugin
// ---------------------------------------------------------------------------

#[async_trait]
impl HostPlugin for D1Table {
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }

    fn world(&self) -> WitWorld {
        // Under `imports`, because this is an interface the plugin offers a
        // workload. Some plugins in this workspace put it under `exports`; see
        // the note in `wit/world.wit`.
        WitWorld {
            imports: HashSet::from([WitInterface::from("custom:d1-table/table@0.1.0")]),
            ..Default::default()
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        let Some(interface) = interfaces.get("custom", "d1-table", &[]) else {
            tracing::warn!(
                "D1Table plugin requested for non-d1-table interface(s): {interfaces:?}"
            );
            return Ok(());
        };

        let interface_config = interface.config.clone();

        bindings::custom::d1_table::types::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;
        bindings::custom::d1_table::table::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;

        let WorkloadItem::Component(component_handle) = item else {
            return Ok(());
        };

        debug!(
            component_id = component_handle.id(),
            "D1Table plugin bound to component"
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
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        self.tracker
            .write()
            .await
            .remove_workload_with_cleanup(workload_id, |_| async {}, |_| async {})
            .await;
        debug!(workload_id = %workload_id, "D1Table plugin unbound");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plugin_id_is_stable() {
        assert_eq!(D1Table::new().id(), PLUGIN_ID);
        assert_eq!(D1Table::default().id(), PLUGIN_ID);
    }

    #[test]
    fn world_advertises_the_interface_as_an_import() {
        let world = D1Table::new().world();
        assert!(
            world
                .imports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "d1-table"),
            "the plugin offers this interface, so it belongs under imports: {:?}",
            world.imports
        );
        assert!(
            world.exports.is_empty(),
            "nothing here is exported to the workload: {:?}",
            world.exports
        );
    }

    #[test]
    fn client_cache_key_separates_databases_and_rotates_with_the_token() {
        let base = client_cache_key("acct", "db1", "token");
        assert_eq!(base, client_cache_key("acct", "db1", "token"));
        assert_ne!(base, client_cache_key("acct", "db2", "token"));
        assert_ne!(base, client_cache_key("other", "db1", "token"));
        assert_ne!(base, client_cache_key("acct", "db1", "rotated"));
        // The token itself must not be recoverable from the key.
        assert!(!base.contains("token"));
    }

    #[test]
    fn build_errors_surface_as_recoverable_component_errors() {
        let err = sql::build_delete("users", &[]).unwrap_err();
        match table_error(err) {
            TableError::InvalidRequest(message) => {
                assert!(message.contains("condition"), "{message}");
            }
            other => panic!("expected a recoverable request error, got {other:?}"),
        }

        let err = sql::build_delete(
            "bad table",
            &[ConditionOp {
                column: "id".to_string(),
                op: SqlComparisonOp::Eq,
                value: Some(serde_json::json!(1)),
            }],
        )
        .unwrap_err();
        match table_error(err) {
            TableError::InvalidIdentifier(message) => {
                assert!(message.contains(sql::IDENTIFIER_PATTERN), "{message}");
            }
            other => panic!("expected a recoverable identifier error, got {other:?}"),
        }
    }

    #[test]
    fn json_values_map_onto_column_values() {
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
        // 3.125 is exactly representable in binary; 3.14 is not, and clippy's
        // approx_constant lint reads it as PI.
        assert!(matches!(
            json_value_to_column_value(serde_json::json!(3.125)),
            ColumnValue::Real(_)
        ));
        assert!(matches!(
            json_value_to_column_value(serde_json::json!("hello")),
            ColumnValue::Text(s) if s == "hello"
        ));
    }

    #[test]
    fn column_values_map_onto_json() {
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
        assert_eq!(
            column_value_to_json(ColumnValue::Blob(vec![1, 2])),
            serde_json::json!([1, 2])
        );
    }

    #[test]
    fn first_count_reads_the_count_column() {
        let ok = QueryResult {
            columns: vec![ColumnMeta {
                name: "COUNT(*)".to_string(),
                column_type: None,
            }],
            rows: vec![ResultRow {
                values: vec![ColumnValue::Integer(7)],
            }],
            rows_affected: 0,
            last_insert_rowid: None,
        };
        assert!(matches!(first_count(&ok), Ok(7)));

        // An empty table still returns a row, holding zero.
        let zero = QueryResult {
            columns: ok.columns.clone(),
            rows: vec![ResultRow {
                values: vec![ColumnValue::Integer(0)],
            }],
            rows_affected: 0,
            last_insert_rowid: None,
        };
        assert!(matches!(first_count(&zero), Ok(0)));

        // Anything else is a protocol violation, not something to guess at.
        let no_rows = QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: 0,
            last_insert_rowid: None,
        };
        assert!(matches!(
            first_count(&no_rows),
            Err(TableError::Unexpected(_))
        ));

        let wrong_type = QueryResult {
            columns: ok.columns.clone(),
            rows: vec![ResultRow {
                values: vec![ColumnValue::Text("many".to_string())],
            }],
            rows_affected: 0,
            last_insert_rowid: None,
        };
        assert!(matches!(
            first_count(&wrong_type),
            Err(TableError::Unexpected(_))
        ));
    }
}
