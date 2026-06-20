//! Multi-Backend KeyValue Host Plugin
//!
//! This module implements a wasmCloud host plugin that provides `wasi:keyvalue@0.2.0-draft`
//! interfaces using multiple backend engines: OpenDAL (redis, memory, fs),
//! Cloudflare Workers KV, and NATS JetStream KV.
//!
//! Backend is selected via the `backend` config key in interface config.
//! If `backend` is not specified, "memory" is used by default.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use cloudflare_workers_kv_sdk_rs::{KvNamespaceClient, KvRequest};
use opendal::Operator;
use opendal::Scheme;
use opentelemetry::metrics::Counter;
use tokio::sync::RwLock;
use tracing::debug;
use wasmtime::component::Resource;

use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::WorkloadItem;
use wash_runtime::plugin::{HostPlugin, WitInterfaces, WorkloadTracker};
use wash_runtime::wit::{WitInterface, WitWorld};

use custom_plugin_nats_utils::build_nats_connect_options;

const PLUGIN_KEYVALUE_ID: &str = "wasi-keyvalue-multi-backend";

// Generate bindings for the keyvalue world with trappable error handling
mod bindings {
    wasmtime::component::bindgen!({
        path: "../wash-runtime/wit",
        world: "keyvalue",
        imports: { default: async | trappable | tracing },
        with: {
            "wasi:keyvalue/store.bucket": super::BucketHandle,
        },
    });
}

use bindings::wasi::keyvalue::store::{Error as StoreError, KeyResponse};

// Helper to convert SDK result to our result type, consuming the error immediately
fn cf_result_to_string<T, E: std::fmt::Display>(result: Result<T, E>) -> Result<T, String> {
    result.map_err(|e| e.to_string())
}

/// Backend engine for KV operations
enum KvEngine {
    /// OpenDAL operator (redis, memory, fs)
    OpenDal(Operator),
    /// Cloudflare KV client
    Cloudflare { client: KvNamespaceClient },
    /// NATS JetStream KV store
    Nats {
        store: Box<async_nats::jetstream::kv::Store>,
    },
}

impl KvEngine {
    fn clone_engine(&self) -> KvEngine {
        match self {
            KvEngine::OpenDal(op) => KvEngine::OpenDal(op.clone()),
            KvEngine::Cloudflare { client } => KvEngine::Cloudflare {
                client: client.clone(),
            },
            KvEngine::Nats { store } => KvEngine::Nats {
                store: Box::new(store.as_ref().clone()),
            },
        }
    }
}

/// Per-component data.
struct ComponentData {
    /// Static interface config from wasmcloud config
    interface_config: HashMap<String, String>,
    /// Lazily-created backend engine
    engine: Option<KvEngine>,
}

/// Resource handle for a KV bucket
#[derive(Clone)]
pub struct BucketHandle {
    /// Backend engine
    engine: Arc<KvEngine>,
    /// Bucket/namespace identifier
    identifier: String,
}

impl BucketHandle {
    /// Build the full storage key with identifier prefix for OpenDAL backends (slash-separated).
    fn full_key(&self, key: &str) -> String {
        if self.identifier.is_empty() {
            key.to_string()
        } else {
            format!("{}/{}", self.identifier, key)
        }
    }

    /// Build the NATS-style dotted key with identifier prefix.
    fn nats_key(&self, key: &str) -> String {
        if self.identifier.is_empty() {
            key.to_string()
        } else {
            format!("{}.{}", self.identifier, key)
        }
    }
}

/// Multi-backend keyvalue plugin
#[derive(Clone)]
pub struct MultiBackendKeyValue {
    /// Per-component state tracker
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
    /// Metrics
    metrics: Arc<KvMetrics>,
}

struct KvMetrics {
    operations_total: Counter<u64>,
}

impl Default for KvMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl KvMetrics {
    fn new() -> Self {
        let meter = opentelemetry::global::meter("wasi-keyvalue-multi-backend");
        let operations_total = meter
            .u64_counter("wasi_keyvalue_operations_total")
            .with_description("Total number of KV operations")
            .build();
        Self { operations_total }
    }
}

impl Default for MultiBackendKeyValue {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiBackendKeyValue {
    /// Create a new multi-backend KV plugin.
    /// Backend is configured per-workload via interface config.
    pub fn new() -> Self {
        let metrics = KvMetrics::new();
        Self {
            tracker: Arc::new(RwLock::new(WorkloadTracker::default())),
            metrics: Arc::new(metrics),
        }
    }

    fn record_operation(&self, operation: &str) {
        let attributes = [opentelemetry::KeyValue::new(
            "operation",
            operation.to_string(),
        )];
        self.metrics.operations_total.add(1, &attributes);
    }

    /// Get or lazily create the backend engine for a component.
    async fn get_or_create_engine(&self, component_id: &str) -> anyhow::Result<KvEngine> {
        // Check if engine already cached
        {
            let lock = self.tracker.read().await;
            if let Some(data) = lock.get_component_data(component_id)
                && let Some(ref engine) = data.engine
            {
                return Ok(engine.clone_engine());
            }
        }

        // Need to create engine from interface config
        let interface_config = {
            let lock = self.tracker.read().await;
            match lock.get_component_data(component_id) {
                Some(data) => data.interface_config.clone(),
                None => {
                    return Err(anyhow::anyhow!(
                        "No KV config found for component '{component_id}'",
                    ));
                }
            }
        };

        let backend = interface_config
            .get("backend")
            .cloned()
            .unwrap_or_else(|| "memory".to_string());

        let engine = match backend.as_str() {
            "cloudflare" => {
                let account_id = interface_config
                    .get("account_id")
                    .cloned()
                    .unwrap_or_default();
                let api_token = interface_config
                    .get("api_token")
                    .cloned()
                    .unwrap_or_default();
                let namespace = interface_config
                    .get("namespace_id")
                    .cloned()
                    .unwrap_or_default();
                let client = KvNamespaceClient::new(&account_id, &api_token, &namespace);
                KvEngine::Cloudflare { client }
            }
            "nats" => {
                let nats_url = interface_config
                    .get("nats_url")
                    .cloned()
                    .unwrap_or_else(|| "nats://127.0.0.1:4222".to_string());
                let bucket = interface_config
                    .get("bucket")
                    .cloned()
                    .unwrap_or_else(|| "default".to_string());
                let opts = build_nats_connect_options(&interface_config)?;
                let client = opts
                    .connect(&nats_url)
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to connect to NATS: {e}"))?;
                let jetstream = async_nats::jetstream::new(client);
                // Idempotent: create_key_value returns the existing bucket if one
                // exists with the same config, so concurrent opens all succeed and
                // a missing bucket is auto-created. Requires stream-create
                // permission even for read-only use; that is acceptable today
                // because the NATS connection is host-owned and not scoped per
                // workload. Ported from upstream wash-runtime commit 03e335116.
                let store = jetstream
                    .create_key_value(async_nats::jetstream::kv::Config {
                        bucket: bucket.clone(),
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("failed to open NATS KV bucket '{bucket}': {e}")
                    })?;
                KvEngine::Nats {
                    store: Box::new(store),
                }
            }
            _ => {
                // OpenDAL backends: redis, memory, fs, etc.
                let scheme = Scheme::from_str(&backend)
                    .map_err(|e| anyhow::anyhow!("unknown backend '{backend}': {e}"))?;
                let iter = interface_config
                    .iter()
                    .filter(|(k, _)| k.as_str() != "backend")
                    .map(|(k, v)| (k.clone(), v.clone()));
                let op = Operator::via_iter(scheme, iter).map_err(|e| {
                    anyhow::anyhow!(
                        "failed to create OpenDAL operator for backend '{backend}': {e}"
                    )
                })?;
                KvEngine::OpenDal(op)
            }
        };

        // Cache the engine (double-check to avoid race)
        {
            let mut lock = self.tracker.write().await;
            // Another task may have created the engine while we were building ours
            if let Some(data) = lock.get_component_data(component_id)
                && let Some(ref existing) = data.engine
            {
                return Ok(existing.clone_engine());
            }
            if let Some(data) = lock.get_component_data_mut(component_id) {
                data.engine = Some(engine.clone_engine());
            }
        }

        Ok(engine)
    }
}

// Implementation for the store interface
impl<'a> bindings::wasi::keyvalue::store::Host for ActiveCtx<'a> {
    async fn open(
        &mut self,
        identifier: String,
    ) -> wasmtime::Result<Result<Resource<BucketHandle>, StoreError>> {
        let Ok(plugin) = self.try_get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(format!(
                "KV plugin not available for component '{}'",
                self.component_id
            ))));
        };
        plugin.record_operation("open");

        let component_id: Arc<str> = self.component_id.clone();

        // Get or create engine for this component
        let engine = match plugin.get_or_create_engine(&component_id).await {
            Ok(e) => e,
            Err(e) => return Ok(Err(StoreError::Other(format!("{e}")))),
        };

        // Resolve bucket identifier with fallback to config
        let bucket_id = {
            let lock = plugin.tracker.read().await;
            if let Some(data) = lock.get_component_data(&component_id) {
                if identifier.is_empty() {
                    let backend = data
                        .interface_config
                        .get("backend")
                        .cloned()
                        .unwrap_or_default();
                    match backend.as_str() {
                        "cloudflare" => data
                            .interface_config
                            .get("namespace_id")
                            .cloned()
                            .unwrap_or_default(),
                        "nats" => data
                            .interface_config
                            .get("bucket")
                            .cloned()
                            .unwrap_or_default(),
                        _ => data
                            .interface_config
                            .get("root")
                            .cloned()
                            .unwrap_or_default(),
                    }
                } else {
                    identifier
                }
            } else {
                identifier
            }
        };

        debug!(
            component_id = %component_id,
            identifier = %bucket_id,
            "Opening KV bucket"
        );

        let bucket = BucketHandle {
            engine: Arc::new(engine),
            identifier: bucket_id,
        };

        let resource = self.table.push(bucket)?;
        Ok(Ok(resource))
    }
}

// Resource host trait implementations for bucket
impl<'a> bindings::wasi::keyvalue::store::HostBucket for ActiveCtx<'a> {
    async fn get(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
    ) -> wasmtime::Result<Result<Option<Vec<u8>>, StoreError>> {
        let Ok(plugin) = self.try_get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(format!(
                "KV plugin not available for component '{}'",
                self.component_id
            ))));
        };
        plugin.record_operation("get");

        let bucket_handle = self.table.get(&bucket)?;
        let full_key = bucket_handle.full_key(&key);

        match bucket_handle.engine.as_ref() {
            KvEngine::OpenDal(op) => match op.read(&full_key).await {
                Ok(data) => Ok(Ok(Some(data.to_vec()))),
                Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(Ok(None)),
                Err(e) => Ok(Err(StoreError::Other(format!("{e}")))),
            },
            KvEngine::Cloudflare { client } => {
                let client = client.clone();
                let result = cf_result_to_string(client.get(&key).await);
                match result {
                    Ok(value) if !value.is_empty() => {
                        if let Ok(decoded) = base64::Engine::decode(
                            &base64::engine::general_purpose::STANDARD,
                            &value,
                        ) {
                            Ok(Ok(Some(decoded)))
                        } else {
                            Ok(Ok(Some(value.into_bytes())))
                        }
                    }
                    Ok(_) => Ok(Ok(Some(vec![]))),
                    Err(e) => Ok(Err(StoreError::Other(e))),
                }
            }
            KvEngine::Nats { store } => {
                let nats_key = bucket_handle.nats_key(&key);
                match store.get(&nats_key).await {
                    Ok(Some(bytes)) => Ok(Ok(Some(bytes.to_vec()))),
                    Ok(None) => Ok(Ok(None)),
                    Err(e) => Ok(Err(StoreError::Other(format!("{e}")))),
                }
            }
        }
    }

    async fn set(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
        value: Vec<u8>,
    ) -> wasmtime::Result<Result<(), StoreError>> {
        let Ok(plugin) = self.try_get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(format!(
                "KV plugin not available for component '{}'",
                self.component_id
            ))));
        };
        plugin.record_operation("set");

        let bucket_handle = self.table.get(&bucket)?;
        let full_key = bucket_handle.full_key(&key);

        match bucket_handle.engine.as_ref() {
            KvEngine::OpenDal(op) => match op.write(&full_key, value).await {
                Ok(_) => Ok(Ok(())),
                Err(e) => Ok(Err(StoreError::Other(format!("{e}")))),
            },
            KvEngine::Cloudflare { client } => {
                let should_base64 = !value.iter().all(|b| b.is_ascii() && *b >= 32);
                let value_str = if should_base64 {
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &value)
                } else {
                    String::from_utf8_lossy(&value).to_string()
                };
                let kv_request = KvRequest::new(&key, &value_str);
                match cf_result_to_string(client.write(kv_request).await) {
                    Ok(_) => Ok(Ok(())),
                    Err(e) => Ok(Err(StoreError::Other(e))),
                }
            }
            KvEngine::Nats { store } => {
                let nats_key = bucket_handle.nats_key(&key);
                match store.put(nats_key, value.into()).await {
                    Ok(_) => Ok(Ok(())),
                    Err(e) => Ok(Err(StoreError::Other(format!("{e}")))),
                }
            }
        }
    }

    async fn delete(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
    ) -> wasmtime::Result<Result<(), StoreError>> {
        let Ok(plugin) = self.try_get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(format!(
                "KV plugin not available for component '{}'",
                self.component_id
            ))));
        };
        plugin.record_operation("delete");

        let bucket_handle = self.table.get(&bucket)?;
        let full_key = bucket_handle.full_key(&key);

        match bucket_handle.engine.as_ref() {
            KvEngine::OpenDal(op) => match op.delete(&full_key).await {
                Ok(_) => Ok(Ok(())),
                Err(e) => Ok(Err(StoreError::Other(format!("{e}")))),
            },
            KvEngine::Cloudflare { client } => {
                match cf_result_to_string(client.delete(&key).await) {
                    Ok(_) => Ok(Ok(())),
                    Err(e) => Ok(Err(StoreError::Other(e))),
                }
            }
            KvEngine::Nats { store } => {
                let nats_key = bucket_handle.nats_key(&key);
                match store.delete(&nats_key).await {
                    Ok(_) => Ok(Ok(())),
                    Err(e) => Ok(Err(StoreError::Other(format!("{e}")))),
                }
            }
        }
    }

    async fn exists(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
    ) -> wasmtime::Result<Result<bool, StoreError>> {
        let Ok(plugin) = self.try_get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(format!(
                "KV plugin not available for component '{}'",
                self.component_id
            ))));
        };
        plugin.record_operation("exists");

        let bucket_handle = self.table.get(&bucket)?;
        let full_key = bucket_handle.full_key(&key);

        match bucket_handle.engine.as_ref() {
            KvEngine::OpenDal(op) => match op.exists(&full_key).await {
                Ok(exists) => Ok(Ok(exists)),
                Err(_) => Ok(Ok(false)),
            },
            KvEngine::Cloudflare { client } => {
                match cf_result_to_string(client.read_metadata(&key).await) {
                    Ok(_) => Ok(Ok(true)),
                    Err(_) => Ok(Ok(false)),
                }
            }
            KvEngine::Nats { store } => {
                let nats_key = bucket_handle.nats_key(&key);
                match store.get(&nats_key).await {
                    Ok(Some(_)) => Ok(Ok(true)),
                    Ok(None) => Ok(Ok(false)),
                    Err(e) => Ok(Err(StoreError::Other(format!("{e}")))),
                }
            }
        }
    }

    async fn list_keys(
        &mut self,
        bucket: Resource<BucketHandle>,
        cursor: Option<u64>,
    ) -> wasmtime::Result<Result<KeyResponse, StoreError>> {
        let Ok(plugin) = self.try_get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(format!(
                "KV plugin not available for component '{}'",
                self.component_id
            ))));
        };
        plugin.record_operation("list_keys");

        let bucket_handle = self.table.get(&bucket)?;
        let prefix = &bucket_handle.identifier;

        match bucket_handle.engine.as_ref() {
            KvEngine::OpenDal(op) => {
                let path = if prefix.is_empty() {
                    "/".to_string()
                } else {
                    format!("{prefix}/")
                };
                match op.list(&path).await {
                    Ok(entries) => {
                        let keys: Vec<String> =
                            entries.into_iter().map(|e| e.name().to_string()).collect();
                        let start_index = cursor.unwrap_or(0) as usize;
                        const PAGE_SIZE: usize = 100;
                        let end_index = std::cmp::min(start_index + PAGE_SIZE, keys.len());
                        let page_keys = keys
                            .get(start_index..end_index)
                            .unwrap_or_default()
                            .to_vec();
                        let next_cursor = if end_index < keys.len() {
                            Some(end_index as u64)
                        } else {
                            None
                        };
                        Ok(Ok(KeyResponse {
                            keys: page_keys,
                            cursor: next_cursor,
                        }))
                    }
                    Err(e) => Ok(Err(StoreError::Other(format!("{e}")))),
                }
            }
            KvEngine::Cloudflare { client } => {
                match cf_result_to_string(client.list_all_keys().await) {
                    Ok(all_keys) => {
                        const PAGE_SIZE: usize = 100;
                        let start_index = cursor.unwrap_or(0) as usize;
                        let end_index = std::cmp::min(start_index + PAGE_SIZE, all_keys.len());
                        let page_keys = all_keys
                            .get(start_index..end_index)
                            .unwrap_or_default()
                            .to_vec();
                        let next_cursor = if end_index < all_keys.len() {
                            Some(end_index as u64)
                        } else {
                            None
                        };
                        Ok(Ok(KeyResponse {
                            keys: page_keys,
                            cursor: next_cursor,
                        }))
                    }
                    Err(e) => Ok(Err(StoreError::Other(e))),
                }
            }
            KvEngine::Nats { store } => {
                let mut keys_iter = match store.keys().await {
                    Ok(i) => i,
                    Err(e) => return Ok(Err(StoreError::Other(format!("{e}")))),
                };
                let prefix_filter = if prefix.is_empty() {
                    "".to_string()
                } else {
                    format!("{prefix}.")
                };
                use futures::StreamExt;
                let mut raw_keys: Vec<String> = Vec::new();
                while let Some(item) = keys_iter.next().await {
                    if let Ok(k) = item {
                        raw_keys.push(k);
                    }
                }
                let all_keys: Vec<String> = raw_keys
                    .into_iter()
                    .filter(|k: &String| prefix_filter.is_empty() || k.starts_with(&prefix_filter))
                    .map(|k: String| {
                        if prefix_filter.is_empty() {
                            k
                        } else {
                            k.strip_prefix(&prefix_filter).unwrap_or(&k).to_string()
                        }
                    })
                    .collect();
                let start_index = cursor.unwrap_or(0) as usize;
                const PAGE_SIZE: usize = 100;
                let end_index = std::cmp::min(start_index + PAGE_SIZE, all_keys.len());
                let page_keys = all_keys
                    .get(start_index..end_index)
                    .unwrap_or_default()
                    .to_vec();
                let next_cursor = if end_index < all_keys.len() {
                    Some(end_index as u64)
                } else {
                    None
                };
                Ok(Ok(KeyResponse {
                    keys: page_keys,
                    cursor: next_cursor,
                }))
            }
        }
    }

    async fn drop(&mut self, rep: Resource<BucketHandle>) -> wasmtime::Result<()> {
        debug!(resource_id = ?rep, "Dropping bucket resource");
        self.table.delete(rep)?;
        Ok(())
    }
}

// Implementation for the atomics interface
impl<'a> bindings::wasi::keyvalue::atomics::Host for ActiveCtx<'a> {
    async fn increment(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
        delta: u64,
    ) -> wasmtime::Result<Result<u64, StoreError>> {
        let Ok(plugin) = self.try_get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(format!(
                "KV plugin not available for component '{}'",
                self.component_id
            ))));
        };
        plugin.record_operation("increment");

        let bucket_handle = self.table.get(&bucket)?;
        let full_key = bucket_handle.full_key(&key);

        match bucket_handle.engine.as_ref() {
            KvEngine::OpenDal(op) => {
                // Read-modify-write
                let current = match op.read(&full_key).await {
                    Ok(data) => {
                        let bytes = data.to_vec();
                        if bytes.len() == 8 {
                            u64::from_be_bytes(bytes.try_into().unwrap_or([0; 8]))
                        } else {
                            String::from_utf8_lossy(&bytes).parse::<u64>().unwrap_or(0)
                        }
                    }
                    Err(_) => 0,
                };
                let new_value = current.saturating_add(delta);
                match op.write(&full_key, new_value.to_be_bytes().to_vec()).await {
                    Ok(_) => Ok(Ok(new_value)),
                    Err(e) => Ok(Err(StoreError::Other(format!("{e}")))),
                }
            }
            KvEngine::Cloudflare { client } => {
                // Read-modify-write (not truly atomic)
                let client_clone = client.clone();
                let get_result = cf_result_to_string(client_clone.get(&key).await);
                let current_value: u64 = match get_result {
                    Ok(value) if !value.is_empty() => {
                        let bytes: Bytes = if let Ok(decoded) = base64::Engine::decode(
                            &base64::engine::general_purpose::STANDARD,
                            &value,
                        ) {
                            Bytes::from(decoded)
                        } else {
                            Bytes::from(value.into_bytes())
                        };
                        if bytes.len() == 8 {
                            bytes.clone().get_u64()
                        } else {
                            String::from_utf8_lossy(&bytes).parse::<u64>().unwrap_or(0)
                        }
                    }
                    _ => 0,
                };
                let new_value = current_value.saturating_add(delta);
                let value_bytes = new_value.to_be_bytes().to_vec();
                let value_str = base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    &value_bytes,
                );
                let kv_request = KvRequest::new(&key, &value_str);
                match cf_result_to_string(client.write(kv_request).await) {
                    Ok(_) => Ok(Ok(new_value)),
                    Err(e) => Ok(Err(StoreError::Other(e))),
                }
            }
            KvEngine::Nats { store } => {
                // CAS via revision check
                let nats_key = bucket_handle.nats_key(&key);
                let (entry_revision, entry_value) = match store.entry(&nats_key).await {
                    Ok(Some(mut e)) => (Some(e.revision), e.value.get_u64()),
                    Ok(None) => (None, 0),
                    Err(e) => return Ok(Err(StoreError::Other(format!("{e}")))),
                };
                let new_value = entry_value + delta;
                let entry_bytes = Bytes::from(new_value.to_be_bytes().to_vec());
                match entry_revision {
                    Some(rev) => match store.update(&nats_key, entry_bytes, rev).await {
                        Ok(_) => Ok(Ok(new_value)),
                        Err(e) => Ok(Err(StoreError::Other(format!("{e}")))),
                    },
                    None => match store.put(nats_key, entry_bytes).await {
                        Ok(_) => Ok(Ok(new_value)),
                        Err(e) => Ok(Err(StoreError::Other(format!("{e}")))),
                    },
                }
            }
        }
    }
}

// Implementation for the batch interface
impl<'a> bindings::wasi::keyvalue::batch::Host for ActiveCtx<'a> {
    async fn get_many(
        &mut self,
        bucket: Resource<BucketHandle>,
        keys: Vec<String>,
    ) -> wasmtime::Result<Result<Vec<Option<(String, Vec<u8>)>>, StoreError>> {
        let Ok(plugin) = self.try_get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(format!(
                "KV plugin not available for component '{}'",
                self.component_id
            ))));
        };
        plugin.record_operation("get_many");

        let bucket_handle = self.table.get(&bucket)?;

        match bucket_handle.engine.as_ref() {
            KvEngine::OpenDal(op) => {
                let mut results = Vec::with_capacity(keys.len());
                for key in keys {
                    let full_key = bucket_handle.full_key(&key);
                    match op.read(&full_key).await {
                        Ok(data) => results.push(Some((key, data.to_vec()))),
                        Err(_) => results.push(None),
                    }
                }
                Ok(Ok(results))
            }
            KvEngine::Cloudflare { client } => {
                let mut results = Vec::with_capacity(keys.len());
                for key in keys {
                    let client_clone = client.clone();
                    let get_result = cf_result_to_string(client_clone.get(&key).await);
                    let result = match get_result {
                        Ok(value) if !value.is_empty() => {
                            if let Ok(decoded) = base64::Engine::decode(
                                &base64::engine::general_purpose::STANDARD,
                                &value,
                            ) {
                                Some((key, decoded))
                            } else {
                                Some((key, value.into_bytes()))
                            }
                        }
                        Ok(_) => Some((key, vec![])),
                        Err(_) => None,
                    };
                    results.push(result);
                }
                Ok(Ok(results))
            }
            KvEngine::Nats { store } => {
                let mut results = Vec::with_capacity(keys.len());
                for key in keys {
                    let nats_key = bucket_handle.nats_key(&key);
                    match store.get(&nats_key).await {
                        Ok(Some(bytes)) => results.push(Some((key, bytes.to_vec()))),
                        Ok(None) => results.push(None),
                        Err(_) => results.push(None),
                    }
                }
                Ok(Ok(results))
            }
        }
    }

    async fn set_many(
        &mut self,
        bucket: Resource<BucketHandle>,
        key_values: Vec<(String, Vec<u8>)>,
    ) -> wasmtime::Result<Result<(), StoreError>> {
        let Ok(plugin) = self.try_get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(format!(
                "KV plugin not available for component '{}'",
                self.component_id
            ))));
        };
        plugin.record_operation("set_many");

        let bucket_handle = self.table.get(&bucket)?;

        match bucket_handle.engine.as_ref() {
            KvEngine::OpenDal(op) => {
                for (key, value) in key_values {
                    let full_key = bucket_handle.full_key(&key);
                    if let Err(e) = op.write(&full_key, value).await {
                        return Ok(Err(StoreError::Other(format!("{e}"))));
                    }
                }
                Ok(Ok(()))
            }
            KvEngine::Cloudflare { client } => {
                let kv_requests: Vec<KvRequest> = key_values
                    .into_iter()
                    .map(|(key, value)| {
                        let should_base64 = !value.iter().all(|b| b.is_ascii() && *b >= 32);
                        let value_str = if should_base64 {
                            base64::Engine::encode(
                                &base64::engine::general_purpose::STANDARD,
                                &value,
                            )
                        } else {
                            String::from_utf8_lossy(&value).to_string()
                        };
                        KvRequest::new(&key, &value_str)
                    })
                    .collect();
                match cf_result_to_string(client.write_multiple(kv_requests).await) {
                    Ok(_) => Ok(Ok(())),
                    Err(e) => Ok(Err(StoreError::Other(e))),
                }
            }
            KvEngine::Nats { store } => {
                for (key, value) in key_values {
                    let nats_key = bucket_handle.nats_key(&key);
                    if let Err(e) = store.put(nats_key, value.into()).await {
                        return Ok(Err(StoreError::Other(format!("{e}"))));
                    }
                }
                Ok(Ok(()))
            }
        }
    }

    async fn delete_many(
        &mut self,
        bucket: Resource<BucketHandle>,
        keys: Vec<String>,
    ) -> wasmtime::Result<Result<(), StoreError>> {
        let Ok(plugin) = self.try_get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(format!(
                "KV plugin not available for component '{}'",
                self.component_id
            ))));
        };
        plugin.record_operation("delete_many");

        let bucket_handle = self.table.get(&bucket)?;

        match bucket_handle.engine.as_ref() {
            KvEngine::OpenDal(op) => {
                for key in keys {
                    let full_key = bucket_handle.full_key(&key);
                    if let Err(e) = op.delete(&full_key).await {
                        return Ok(Err(StoreError::Other(format!("{e}"))));
                    }
                }
                Ok(Ok(()))
            }
            KvEngine::Cloudflare { client } => {
                let keys_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                match cf_result_to_string(client.delete_multiple(keys_refs).await) {
                    Ok(_) => Ok(Ok(())),
                    Err(e) => Ok(Err(StoreError::Other(e))),
                }
            }
            KvEngine::Nats { store } => {
                for key in keys {
                    let nats_key = bucket_handle.nats_key(&key);
                    if let Err(e) = store.delete(&nats_key).await {
                        return Ok(Err(StoreError::Other(format!("{e}"))));
                    }
                }
                Ok(Ok(()))
            }
        }
    }
}

#[async_trait]
impl HostPlugin for MultiBackendKeyValue {
    fn id(&self) -> &'static str {
        PLUGIN_KEYVALUE_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: HashSet::from([WitInterface::from(
                "wasi:keyvalue/store,atomics,batch@0.2.0-draft",
            )]),
            ..Default::default()
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        let Some(interface) = interfaces.get("wasi", "keyvalue", &[]) else {
            tracing::warn!(
                "KV plugin requested for non-wasi:keyvalue interface(s): {:?}",
                interfaces
            );
            return Ok(());
        };

        let interface_config = interface.config.clone();

        let linker = item.linker();
        bindings::wasi::keyvalue::store::add_to_linker::<_, SharedCtx>(linker, extract_active_ctx)?;
        bindings::wasi::keyvalue::atomics::add_to_linker::<_, SharedCtx>(
            linker,
            extract_active_ctx,
        )?;
        bindings::wasi::keyvalue::batch::add_to_linker::<_, SharedCtx>(linker, extract_active_ctx)?;

        let WorkloadItem::Component(component_handle) = item else {
            return Ok(());
        };

        debug!(
            component_id = component_handle.id(),
            "KV plugin bound to component"
        );

        self.tracker.write().await.add_component(
            component_handle,
            ComponentData {
                interface_config,
                engine: None,
            },
        );

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
        debug!(workload_id = %workload_id, "KV plugin unbound");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_id() {
        let plugin = MultiBackendKeyValue::new();
        assert_eq!(plugin.id(), PLUGIN_KEYVALUE_ID);
    }

    #[test]
    fn test_world_exports() {
        let plugin = MultiBackendKeyValue::new();
        let world = plugin.world();
        assert!(
            world
                .exports
                .iter()
                .any(|i| i.namespace == "wasi" && i.package == "keyvalue")
        );
    }

    #[test]
    fn test_default() {
        let plugin = MultiBackendKeyValue::default();
        assert_eq!(plugin.id(), PLUGIN_KEYVALUE_ID);
    }
}
