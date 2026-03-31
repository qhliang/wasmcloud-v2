//! # Cloudflare KV Host Plugin
//!
//! This module implements a wasmCloud host plugin that provides `wasi:keyvalue@0.2.0-draft`
//! interfaces using Cloudflare Workers KV as the backend storage.
//!
//! ## Usage
//!
//! ```ignore
//! use custom_plugin_cf_kv::CloudflareKeyValue;
//! use wash_runtime::host::HostBuilder;
//! use std::sync::Arc;
//!
//! // Create the plugin (credentials are configured per-workload via interface config)
//! let cf_kv = CloudflareKeyValue::new();
//!
//! // Add to host builder
//! let host = HostBuilder::new()
//!     .with_plugin(Arc::new(cf_kv))?
//!     .build()?;
//! ```
//!
//! ## Per-Workload Configuration
//!
//! Each workload must configure its credentials and namespace via interface config:
//!
//! ```ignore
//! // In the workload manifest or interface configuration:
//! // wasi:keyvalue:
//! //   config:
//! //     account_id: "your-cloudflare-account-id"
//! //     api_token: "your-cloudflare-api-token"
//! //     namespace: "your-kv-namespace-id"
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use cloudflare_workers_kv_sdk_rs::{KvNamespaceClient, KvRequest};
use opentelemetry::metrics::Counter;
use tokio::sync::RwLock;
use tracing::debug;
use wasmtime::component::Resource;

use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::WorkloadItem;
use wash_runtime::plugin::HostPlugin;
use wash_runtime::wit::{WitInterface, WitWorld};

const PLUGIN_KEYVALUE_ID: &str = "wasi-keyvalue-cf-kv";

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

/// Configuration for Cloudflare KV (per-workload)
#[derive(Clone, Debug)]
pub struct CloudflareWorkloadConfig {
    /// Cloudflare account ID
    pub account_id: String,
    /// Cloudflare API token with KV permissions
    pub api_token: String,
    /// KV namespace ID
    pub namespace: String,
}

/// Resource handle for a Cloudflare KV bucket
#[derive(Clone)]
pub struct BucketHandle {
    /// Cloudflare KV client
    client: KvNamespaceClient,
    /// Bucket/namespace identifier
    identifier: String,
}

/// Cloudflare KV-based keyvalue plugin
#[derive(Clone, Default)]
pub struct CloudflareKeyValue {
    /// Mapping from workload_id to workload-specific config
    configs: Arc<RwLock<HashMap<String, CloudflareWorkloadConfig>>>,
    /// Metrics
    metrics: Arc<CloudflareKvMetrics>,
}

struct CloudflareKvMetrics {
    operations_total: Counter<u64>,
}

impl Default for CloudflareKvMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl CloudflareKvMetrics {
    fn new() -> Self {
        let meter = opentelemetry::global::meter("wasi-keyvalue-cf");
        let operations_total = meter
            .u64_counter("wasi_keyvalue_cf_operations_total")
            .with_description("Total number of operations performed on the Cloudflare KV store")
            .build();
        Self { operations_total }
    }
}

impl CloudflareKeyValue {
    /// Create a new Cloudflare KV plugin
    /// Credentials and namespace are configured per-workload via interface config
    pub fn new() -> Self {
        let metrics = CloudflareKvMetrics::new();
        Self {
            configs: Arc::new(RwLock::new(HashMap::new())),
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
}

// Helper to convert SDK result to our result type, consuming the error immediately
fn cf_result_to_string<T, E: std::fmt::Display>(result: Result<T, E>) -> Result<T, String> {
    result.map_err(|e| e.to_string())
}

// Implementation for the store interface
impl<'a> bindings::wasi::keyvalue::store::Host for ActiveCtx<'a> {
    async fn open(
        &mut self,
        identifier: String,
    ) -> wasmtime::Result<Result<Resource<BucketHandle>, StoreError>> {
        let Some(plugin) = self.get_plugin::<CloudflareKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "Cloudflare KV keyvalue plugin not available".to_string(),
            )));
        };
        plugin.record_operation("open");

        let workload_id = self.workload_id.as_ref().to_string();

        // Get the config for this workload
        let configs = plugin.configs.read().await;
        let config = match configs.get(&workload_id) {
            Some(cfg) => cfg.clone(),
            None => {
                return Ok(Err(StoreError::Other(format!(
                    "No Cloudflare KV config found for workload '{}'. \
                     Please configure account_id, api_token, and namespace in interface config.",
                    workload_id
                ))));
            }
        };
        drop(configs);

        debug!(
           workload_id = %workload_id,
           namespace = %config.namespace,
            identifier = %identifier,
            "Opening Cloudflare KV bucket"
        );

        // Create client with the workload-specific config
        let client =
            KvNamespaceClient::new(&config.account_id, &config.api_token, &config.namespace);
        let bucket = BucketHandle {
            client,
            identifier: identifier.clone(),
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
        let Some(plugin) = self.get_plugin::<CloudflareKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "Cloudflare KV keyvalue plugin not available".to_string(),
            )));
        };
        plugin.record_operation("get");

        let bucket_handle = self.table.get(&bucket)?;
        debug!(
            workload_id = self.workload_id.as_ref(),
            key = %key,
            identifier = %bucket_handle.identifier,
            "Getting value from Cloudflare KV"
        );

        // Clone client for async use and convert error immediately to String
        let client = bucket_handle.client.clone();
        let result = cf_result_to_string(client.get(&key).await);

        match result {
            Ok(value) if !value.is_empty() => {
                // Try to decode from base64 first (for binary data)
                if let Ok(decoded) =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &value)
                {
                    Ok(Ok(Some(decoded)))
                } else {
                    // Treat as plain string
                    Ok(Ok(Some(value.into_bytes())))
                }
            }
            Ok(_) => {
                // Empty response - treat as empty value (key exists with empty value)
                Ok(Ok(Some(vec![])))
            }
            Err(e) => {
                debug!("Cloudflare KV error getting key '{}': {}", key, e);
                Ok(Err(StoreError::Other(e)))
            }
        }
    }

    async fn set(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
        value: Vec<u8>,
    ) -> wasmtime::Result<Result<(), StoreError>> {
        let Some(plugin) = self.get_plugin::<CloudflareKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "Cloudflare KV keyvalue plugin not available".to_string(),
            )));
        };
        plugin.record_operation("set");

        let bucket_handle = self.table.get(&bucket)?;
        debug!(
            workload_id = self.workload_id.as_ref(),
            key = %key,
            identifier = %bucket_handle.identifier,
            "Setting value in Cloudflare KV"
        );

        // Cloudflare KV requires values to be text or base64-encoded
        let should_base64 = !value.iter().all(|b| b.is_ascii() && *b >= 32);
        let value_str = if should_base64 {
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &value)
        } else {
            String::from_utf8_lossy(&value).to_string()
        };

        let kv_request = KvRequest::new(&key, &value_str);
        let result = cf_result_to_string(bucket_handle.client.write(kv_request).await);

        match result {
            Ok(_) => Ok(Ok(())),
            Err(e) => {
                debug!("Cloudflare KV error setting key '{}': {}", key, e);
                Ok(Err(StoreError::Other(e)))
            }
        }
    }

    async fn delete(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
    ) -> wasmtime::Result<Result<(), StoreError>> {
        let Some(plugin) = self.get_plugin::<CloudflareKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "Cloudflare KV keyvalue plugin not available".to_string(),
            )));
        };
        plugin.record_operation("delete");

        let bucket_handle = self.table.get(&bucket)?;
        debug!(
            workload_id = self.workload_id.as_ref(),
            key = %key,
            identifier = %bucket_handle.identifier,
            "Deleting value from Cloudflare KV"
        );

        let result = cf_result_to_string(bucket_handle.client.delete(&key).await);

        match result {
            Ok(_) => Ok(Ok(())),
            Err(e) => {
                debug!("Cloudflare KV error deleting key '{}': {}", key, e);
                Ok(Err(StoreError::Other(e)))
            }
        }
    }

    async fn exists(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
    ) -> wasmtime::Result<Result<bool, StoreError>> {
        let Some(plugin) = self.get_plugin::<CloudflareKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "Cloudflare KV keyvalue plugin not available".to_string(),
            )));
        };
        plugin.record_operation("exists");

        let bucket_handle = self.table.get(&bucket)?;
        debug!(
            workload_id = self.workload_id.as_ref(),
            key = %key,
            identifier = %bucket_handle.identifier,
            "Checking if key exists in Cloudflare KV"
        );

        // Cloudflare KV doesn't have a native exists method, so we use read_metadata
        let result = cf_result_to_string(bucket_handle.client.read_metadata(&key).await);

        match result {
            Ok(_) => Ok(Ok(true)),
            Err(_) => Ok(Ok(false)),
        }
    }

    async fn list_keys(
        &mut self,
        bucket: Resource<BucketHandle>,
        cursor: Option<u64>,
    ) -> wasmtime::Result<Result<KeyResponse, StoreError>> {
        let Some(plugin) = self.get_plugin::<CloudflareKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "Cloudflare KV keyvalue plugin not available".to_string(),
            )));
        };
        plugin.record_operation("list_keys");

        let bucket_handle = self.table.get(&bucket)?;
        debug!(
            workload_id = self.workload_id.as_ref(),
            identifier = %bucket_handle.identifier,
            cursor = ?cursor,
            "Listing keys from Cloudflare KV"
        );

        // Cloudflare KV's list_all_keys doesn't support cursor pagination directly
        // We implement client-side pagination
        let result = cf_result_to_string(bucket_handle.client.list_all_keys().await);

        match result {
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
            Err(e) => {
                debug!("Cloudflare KV error listing keys: {}", e);
                Ok(Err(StoreError::Other(e)))
            }
        }
    }

    async fn drop(&mut self, rep: Resource<BucketHandle>) -> wasmtime::Result<()> {
        debug!(
            workload_id = self.workload_id.as_ref(),
            resource_id = ?rep,
            "Dropping bucket resource"
        );
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
        let Some(plugin) = self.get_plugin::<CloudflareKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "Cloudflare KV keyvalue plugin not available".to_string(),
            )));
        };
        plugin.record_operation("increment");

        let bucket_handle = self.table.get(&bucket)?;
        debug!(
            workload_id = self.workload_id.as_ref(),
            key = %key,
            delta = delta,
            identifier = %bucket_handle.identifier,
            "Incrementing value in Cloudflare KV"
        );

        // Cloudflare KV doesn't support atomic operations natively
        // We implement a simple read-modify-write pattern
        // Note: This is NOT truly atomic and may have race conditions

        // Get current value - clone client for async use and convert error immediately
        let client = bucket_handle.client.clone();
        let get_result = cf_result_to_string(client.get(&key).await);

        let current_value: u64 = match get_result {
            Ok(value) if !value.is_empty() => {
                // Try to decode from base64 first (for binary data)
                let bytes: Bytes = if let Ok(decoded) =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &value)
                {
                    Bytes::from(decoded)
                } else {
                    Bytes::from(value.into_bytes())
                };

                // Try to parse as u64 from big-endian bytes
                if bytes.len() == 8 {
                    bytes.clone().get_u64()
                } else {
                    // Try to parse as string representation
                    String::from_utf8_lossy(&bytes).parse::<u64>().unwrap_or(0)
                }
            }
            _ => 0,
        };

        let new_value = current_value.saturating_add(delta);
        let value_bytes = new_value.to_be_bytes().to_vec();
        let value_str =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &value_bytes);
        let kv_request = KvRequest::new(&key, &value_str);

        // Get fresh reference to bucket_handle for write
        let bucket_handle = self.table.get(&bucket)?;
        let write_result = cf_result_to_string(bucket_handle.client.write(kv_request).await);

        match write_result {
            Ok(_) => Ok(Ok(new_value)),
            Err(e) => {
                debug!("Cloudflare KV error incrementing key '{}': {}", key, e);
                Ok(Err(StoreError::Other(e)))
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
        let Some(plugin) = self.get_plugin::<CloudflareKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "Cloudflare KV keyvalue plugin not available".to_string(),
            )));
        };
        plugin.record_operation("get_many");

        let bucket_handle = self.table.get(&bucket)?;
        debug!(
            workload_id = self.workload_id.as_ref(),
            keys_count = keys.len(),
            identifier = %bucket_handle.identifier,
            "Getting multiple values from Cloudflare KV"
        );

        // Cloudflare KV doesn't support batch get natively, so we do sequential requests
        // to avoid Send issues with the SDK's error type
        let mut results = Vec::with_capacity(keys.len());

        for key in keys {
            // Get fresh reference each iteration and convert error immediately
            let bucket_handle = self.table.get(&bucket)?;
            let get_result = cf_result_to_string(bucket_handle.client.get(&key).await);

            let result = match get_result {
                Ok(value) if !value.is_empty() => {
                    // Try to decode from base64 first
                    if let Ok(decoded) =
                        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &value)
                    {
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

    async fn set_many(
        &mut self,
        bucket: Resource<BucketHandle>,
        key_values: Vec<(String, Vec<u8>)>,
    ) -> wasmtime::Result<Result<(), StoreError>> {
        let Some(plugin) = self.get_plugin::<CloudflareKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "Cloudflare KV keyvalue plugin not available".to_string(),
            )));
        };
        plugin.record_operation("set_many");

        let bucket_handle = self.table.get(&bucket)?;
        debug!(
            workload_id = self.workload_id.as_ref(),
            key_values_count = key_values.len(),
            identifier = %bucket_handle.identifier,
            "Setting multiple values in Cloudflare KV"
        );

        // Use the SDK's write_multiple method for batch writes
        let kv_requests: Vec<KvRequest> = key_values
            .into_iter()
            .map(|(key, value)| {
                let should_base64 = !value.iter().all(|b| b.is_ascii() && *b >= 32);
                let value_str = if should_base64 {
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &value)
                } else {
                    String::from_utf8_lossy(&value).to_string()
                };
                KvRequest::new(&key, &value_str)
            })
            .collect();

        let result = cf_result_to_string(bucket_handle.client.write_multiple(kv_requests).await);

        match result {
            Ok(_) => Ok(Ok(())),
            Err(e) => {
                debug!("Cloudflare KV error setting multiple keys: {}", e);
                Ok(Err(StoreError::Other(e)))
            }
        }
    }

    async fn delete_many(
        &mut self,
        bucket: Resource<BucketHandle>,
        keys: Vec<String>,
    ) -> wasmtime::Result<Result<(), StoreError>> {
        let Some(plugin) = self.get_plugin::<CloudflareKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "Cloudflare KV keyvalue plugin not available".to_string(),
            )));
        };
        plugin.record_operation("delete_many");

        let bucket_handle = self.table.get(&bucket)?;
        debug!(
            workload_id = self.workload_id.as_ref(),
            keys_count = keys.len(),
            identifier = %bucket_handle.identifier,
            "Deleting multiple values from Cloudflare KV"
        );

        // Use the SDK's delete_multiple method for batch deletes
        let keys_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let result = cf_result_to_string(bucket_handle.client.delete_multiple(keys_refs).await);

        match result {
            Ok(_) => Ok(Ok(())),
            Err(e) => {
                debug!("Cloudflare KV error deleting multiple keys: {}", e);
                Ok(Err(StoreError::Other(e)))
            }
        }
    }
}

#[async_trait]
impl HostPlugin for CloudflareKeyValue {
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
        component_handle: &mut WorkloadItem<'a>,
        interfaces: std::collections::HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        // Find the wasi:keyvalue interface
        let keyvalue_interface = interfaces
            .iter()
            .find(|i| i.namespace == "wasi" && i.package == "keyvalue");

        let Some(interface) = keyvalue_interface else {
            tracing::warn!(
                "CloudflareKeyValue plugin requested for non-wasi:keyvalue interface(s): {:?}",
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
                    "No 'account_id' configured for wasi:keyvalue interface"
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
                    "No 'api_token' configured for wasi:keyvalue interface"
                );
                String::new()
            });

        let namespace = interface
            .config
            .get("namespace_id")
            .cloned()
            .unwrap_or_else(|| {
                tracing::error!(
                    workload_id = %workload_id,
                    "No 'namespace_id' configured for wasi:keyvalue interface"
                );
                String::new()
            });

        if account_id.is_empty() || api_token.is_empty() || namespace.is_empty() {
            tracing::error!(
                workload_id = %workload_id,
                "Cloudflare KV plugin bound with incomplete config. \
                 Required: account_id, api_token, namespace"
            );
        }

        debug!(
            workload_id = %workload_id,
            account_id = %account_id,
            namespace = %namespace,
            "Configuring Cloudflare KV for workload"
        );

        // Save the config for this workload
        {
            let mut configs = self.configs.write().await;
            configs.insert(
                workload_id.clone(),
                CloudflareWorkloadConfig {
                    account_id,
                    api_token,
                    namespace,
                },
            );
        }

        debug!(
           workload_id = %workload_id,
            "Adding Cloudflare KV keyvalue interfaces to linker for workload"
        );
        let linker = component_handle.linker();

        bindings::wasi::keyvalue::store::add_to_linker::<_, SharedCtx>(linker, extract_active_ctx)?;
        bindings::wasi::keyvalue::atomics::add_to_linker::<_, SharedCtx>(
            linker,
            extract_active_ctx,
        )?;
        bindings::wasi::keyvalue::batch::add_to_linker::<_, SharedCtx>(linker, extract_active_ctx)?;

        debug!(
           workload_id = %workload_id,
            "Successfully added Cloudflare KV keyvalue interfaces to linker for workload"
        );

        debug!("CloudflareKeyValue plugin bound to workload '{workload_id}'");

        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: std::collections::HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        // Clean up the config for this workload
        {
            let mut configs = self.configs.write().await;
            configs.remove(workload_id);
        }
        debug!("CloudflareKeyValue plugin unbound from workload '{workload_id}'");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_id() {
        let plugin = CloudflareKeyValue::new();
        assert_eq!(plugin.id(), "wasi-keyvalue-cf");
    }

    #[test]
    fn test_world_imports() {
        let plugin = CloudflareKeyValue::new();
        let world = plugin.world();

        assert!(
            world
                .imports
                .iter()
                .any(|i| i.namespace == "wasi" && i.package == "keyvalue")
        );
    }

    #[test]
    fn test_default() {
        let plugin = CloudflareKeyValue::default();
        assert_eq!(plugin.id(), "wasi-keyvalue-cf");
    }
}
