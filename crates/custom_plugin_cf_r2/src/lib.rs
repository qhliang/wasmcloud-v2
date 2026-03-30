//! Cloudflare R2 Host Plugin
//!
//! This module implements a wasmCloud host plugin that provides `wasi:blobstore@0.2.0-draft`
//! interfaces using Cloudflare R2 as the backend storage.
//!
//! ## Usage
//!
//! ```ignore
//! use custom_plugin_cf_r2::CloudflareR2;
//! use wash_runtime::host::HostBuilder;
//! use std::sync::Arc;
//!
//! // Create the plugin (credentials are configured per-workload via interface config)
//! let cf_r2 = CloudflareR2::new();
//!
//! // Add to host builder
//! let host = HostBuilder::new()
//!     .with_plugin(Arc::new(cf_r2))?
//!     .build()?;
//! ```
//!
//! ## Per-Workload Configuration
//!
//! Each workload must configure its credentials via interface config:
//!
//! ```ignore
//! // In the workload manifest or interface configuration:
//! // wasi:blobstore:
//! //   config:
//! //     account_id: "your-cloudflare-account-id"
//! //     access_key: "your-r2-access-key-id"
//! //     secret_key: "your-r2-secret-access-key"
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use opentelemetry::metrics::Counter;
use s3::{Auth, Client, Credentials, providers::R2Endpoint};
use tokio::sync::RwLock;
use tracing::debug;
use wasmtime::component::Resource;
use wasmtime_wasi::p2::{
    InputStream, OutputStream,
    pipe::{MemoryInputPipe, MemoryOutputPipe},
};

use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::WorkloadItem;
use wash_runtime::plugin::HostPlugin;
use wash_runtime::wit::{WitInterface, WitWorld};

const PLUGIN_ID: &str = "wasi-blobstore-cf-r2";

mod bindings {
    wasmtime::component::bindgen!({
        path: "../wash-runtime/wit",
        world: "blobstore",
        imports: { default: async | trappable | tracing },
        with: {
            "wasi:io": ::wasmtime_wasi_io::bindings::wasi::io,
            "wasi:blobstore/container.container": String,
            "wasi:blobstore/container.stream-object-names": crate::StreamObjectNamesHandle,
            "wasi:blobstore/types.incoming-value": crate::IncomingValueHandle,
            "wasi:blobstore/types.outgoing-value": crate::OutgoingValueHandle,
        },
    });
}

use bindings::wasi::blobstore::{
    container::Error as ContainerError,
    types::{
        ContainerMetadata, ContainerName, Error as BlobstoreError, ObjectId, ObjectMetadata,
        ObjectName,
    },
};

/// Stream object names resource handle
pub struct StreamObjectNamesHandle {
    pub objects: Vec<String>,
    pub position: usize,
}

/// Incoming value resource handle (data being read)
pub type IncomingValueHandle = Vec<u8>;

/// Outgoing value resource handle (data being written)
pub struct OutgoingValueHandle {
    pub pipe: MemoryOutputPipe,
    pub container_name: Option<String>,
    pub object_name: Option<String>,
}

/// Configuration for Cloudflare R2 (per-workload)
#[derive(Clone, Debug)]
pub struct CloudflareR2Config {
    pub account_id: String,
    pub access_key: String,
    pub secret_key: String,
}

/// Cloudflare R2 blobstore plugin
#[derive(Clone, Default)]
pub struct CloudflareR2 {
    /// Per-workload configurations
    configs: Arc<RwLock<HashMap<String, CloudflareR2Config>>>,
    /// S3 clients per workload, keyed by workload_id
    clients: Arc<RwLock<HashMap<String, Client>>>,
    metrics: Arc<CloudflareR2Metrics>,
}

struct CloudflareR2Metrics {
    operations_total: Counter<u64>,
}

impl Default for CloudflareR2Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl CloudflareR2Metrics {
    fn new() -> Self {
        let meter = opentelemetry::global::meter("wasi-blobstore-cf-r2");
        let operations_total = meter
            .u64_counter("wasi_blobstore_cf_r2_operations_total")
            .with_description("Total number of operations performed on the Cloudflare R2 blobstore")
            .build();
        Self { operations_total }
    }
}

impl CloudflareR2 {
    /// Create a new Cloudflare R2 plugin
    /// Credentials are configured per-workload via interface config
    pub fn new() -> Self {
        let metrics = CloudflareR2Metrics::new();
        Self {
            configs: Arc::new(RwLock::new(HashMap::new())),
            clients: Arc::new(RwLock::new(HashMap::new())),
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
            .ok_or_else(|| anyhow::anyhow!("No config found for workload '{}'", workload_id))?;

        let account_id = config.account_id.clone();
        let creds = Credentials::new(config.access_key.clone(), config.secret_key.clone())?;
        drop(configs);

        // Build the R2 client
        let preset = s3::providers::cloudflare_r2(&account_id, R2Endpoint::Global)?;
        let client = preset
            .async_client_builder()?
            .auth(Auth::Static(creds))
            .build()?;

        // Cache the client
        {
            let mut clients = self.clients.write().await;
            clients.insert(workload_id.to_string(), client.clone());
        }

        Ok(client)
    }
}

#[async_trait]
impl HostPlugin for CloudflareR2 {
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            imports: HashSet::from([WitInterface::from(
                "wasi:blobstore/blobstore,container,types@0.2.0-draft",
            )]),
            ..Default::default()
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        component_handle: &mut WorkloadItem<'a>,
        interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        // Find the wasi:blobstore interface
        let blobstore_interface = interfaces
            .iter()
            .find(|i| i.namespace == "wasi" && i.package == "blobstore");

        let Some(interface) = blobstore_interface else {
            tracing::warn!(
                "CloudflareR2 plugin requested for non-wasi:blobstore interface(s): {:?}",
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
                    "No 'account_id' configured for wasi:blobstore interface"
                );
                String::new()
            });

        let access_key = interface
            .config
            .get("access_key")
            .cloned()
            .unwrap_or_else(|| {
                tracing::error!(
                    workload_id = %workload_id,
                    "No 'access_key' configured for wasi:blobstore interface"
                );
                String::new()
            });

        let secret_key = interface
            .config
            .get("secret_key")
            .cloned()
            .unwrap_or_else(|| {
                tracing::error!(
                    workload_id = %workload_id,
                    "No 'secret_key' configured for wasi:blobstore interface"
                );
                String::new()
            });

        if account_id.is_empty() || access_key.is_empty() || secret_key.is_empty() {
            tracing::error!(
                workload_id = %workload_id,
                "Cloudflare R2 plugin bound with incomplete config. Required: account_id, access_key, secret_key"
            );
        }

        debug!(
            workload_id = %workload_id,
            account_id = %account_id,
            "Configuring Cloudflare R2 for workload"
        );

        // Save the config for this workload
        {
            let mut configs = self.configs.write().await;
            configs.insert(
                workload_id.clone(),
                CloudflareR2Config {
                    account_id,
                    access_key,
                    secret_key,
                },
            );
        }

        debug!(
           workload_id = %workload_id,
            "Adding Cloudflare R2 blobstore interfaces to linker for workload"
        );
        let linker = component_handle.linker();

        bindings::wasi::blobstore::blobstore::add_to_linker::<_, SharedCtx>(
            linker,
            extract_active_ctx,
        )?;
        bindings::wasi::blobstore::container::add_to_linker::<_, SharedCtx>(
            linker,
            extract_active_ctx,
        )?;
        bindings::wasi::blobstore::types::add_to_linker::<_, SharedCtx>(
            linker,
            extract_active_ctx,
        )?;

        debug!("CloudflareR2 plugin bound to workload '{workload_id}'");

        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        // Clean up config and client for this workload
        {
            let mut configs = self.configs.write().await;
            configs.remove(workload_id);
        }
        {
            let mut clients = self.clients.write().await;
            clients.remove(workload_id);
        }

        debug!("CloudflareR2 plugin unbound from workload '{workload_id}'");
        Ok(())
    }
}

// ============================================================================
// Blobstore Interface Implementation
// ============================================================================

impl<'a> bindings::wasi::blobstore::blobstore::Host for ActiveCtx<'a> {
    async fn create_container(
        &mut self,
        name: ContainerName,
    ) -> wasmtime::Result<Result<Resource<String>, BlobstoreError>> {
        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err(
                "Cloudflare R2 blobstore plugin not available".to_string()
            ));
        };
        plugin.record_operation("create_container");

        debug!(
            workload_id = self.workload_id.as_ref(),
            container_name = name,
            "Creating container"
        );

        // Note: Cloudflare R2 doesn't support creating buckets via API
        // We assume buckets are pre-created, so we just return the container resource
        let resource = self.table.push(name)?;
        Ok(Ok(resource))
    }

    async fn get_container(
        &mut self,
        name: ContainerName,
    ) -> wasmtime::Result<Result<Resource<String>, BlobstoreError>> {
        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err(
                "Cloudflare R2 blobstore plugin not available".to_string()
            ));
        };
        plugin.record_operation("get_container");

        debug!(
            workload_id = self.workload_id.as_ref(),
            container_name = name,
            "Getting container"
        );

        // Just return the container resource - actual validation happens on operations
        let resource = self.table.push(name)?;
        Ok(Ok(resource))
    }

    async fn container_exists(
        &mut self,
        name: ContainerName,
    ) -> wasmtime::Result<Result<bool, BlobstoreError>> {
        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err(
                "Cloudflare R2 blobstore plugin not available".to_string()
            ));
        };
        plugin.record_operation("container_exists");

        // Try to list objects in the bucket to verify it exists
        let client = match plugin.get_or_create_client(self.workload_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(format!("failed to get client: {}", e))),
        };

        match client.objects().list_v2(&name).max_keys(1).send().await {
            Ok(_) => Ok(Ok(true)),
            Err(e) => {
                debug!("Container exists check failed: {}", e);
                Ok(Ok(false))
            }
        }
    }

    async fn delete_container(
        &mut self,
        name: ContainerName,
    ) -> wasmtime::Result<Result<(), BlobstoreError>> {
        let Some(_plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err(
                "Cloudflare R2 blobstore plugin not available".to_string()
            ));
        };

        debug!(
            workload_id = self.workload_id.as_ref(),
            container_name = name,
            "Deleting container"
        );

        // Note: Cloudflare R2 doesn't support deleting buckets via API
        debug!(
            "Cloudflare R2 does not support deleting buckets via API. Use the Cloudflare dashboard."
        );

        Ok(Ok(()))
    }

    async fn copy_object(
        &mut self,
        src: ObjectId,
        dest: ObjectId,
    ) -> wasmtime::Result<Result<(), BlobstoreError>> {
        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err(
                "Cloudflare R2 blobstore plugin not available".to_string()
            ));
        };
        plugin.record_operation("copy_object");

        let client = match plugin.get_or_create_client(self.workload_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(format!("failed to get client: {}", e))),
        };

        // Get source object data
        let get_result = match client
            .objects()
            .get(&src.container, &src.object)
            .send()
            .await
        {
            Ok(obj) => obj,
            Err(e) => return Ok(Err(format!("failed to get source object: {}", e))),
        };

        let data = match get_result.bytes().await {
            Ok(bytes) => bytes.to_vec(),
            Err(e) => return Ok(Err(format!("failed to read source object bytes: {}", e))),
        };

        // Upload to destination
        if let Err(e) = client
            .objects()
            .put(&dest.container, &dest.object)
            .body_bytes(data)
            .send()
            .await
        {
            return Ok(Err(format!("failed to put dest object: {}", e)));
        }

        Ok(Ok(()))
    }

    async fn move_object(
        &mut self,
        src: ObjectId,
        dest: ObjectId,
    ) -> wasmtime::Result<Result<(), BlobstoreError>> {
        // First copy the object
        let copy_result = self.copy_object(src.clone(), dest).await?;
        if copy_result.is_err() {
            return Ok(copy_result);
        }

        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err(
                "Cloudflare R2 blobstore plugin not available".to_string()
            ));
        };

        let client = match plugin.get_or_create_client(self.workload_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(format!("failed to get client: {}", e))),
        };

        // Then delete the source
        if let Err(e) = client
            .objects()
            .delete(&src.container, &src.object)
            .send()
            .await
        {
            return Ok(Err(format!("failed to delete source object: {}", e)));
        }

        Ok(Ok(()))
    }
}

// ============================================================================
// Container Interface Implementation
// ============================================================================

impl<'a> bindings::wasi::blobstore::container::HostContainer for ActiveCtx<'a> {
    async fn name(
        &mut self,
        container: Resource<String>,
    ) -> wasmtime::Result<Result<String, ContainerError>> {
        let container_name = self.table.get(&container)?;
        Ok(Ok(container_name.clone()))
    }

    async fn info(
        &mut self,
        container: Resource<String>,
    ) -> wasmtime::Result<Result<ContainerMetadata, ContainerError>> {
        let container_name = self.table.get(&container)?;
        Ok(Ok(ContainerMetadata {
            name: container_name.clone(),
            created_at: 0, // R2 doesn't provide bucket creation time via API
        }))
    }

    async fn get_data(
        &mut self,
        container: Resource<String>,
        name: ObjectName,
        start: u64,
        end: u64,
    ) -> wasmtime::Result<Result<Resource<IncomingValueHandle>, ContainerError>> {
        let container_name = self.table.get(&container)?;

        debug!(
            container = container_name,
            object = name,
            start = start,
            end = end,
            workload_id = self.id,
            "Getting object data from container"
        );

        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err(
                "Cloudflare R2 blobstore plugin not available".to_string()
            ));
        };

        let client = match plugin.get_or_create_client(self.workload_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(format!("failed to get client: {}", e))),
        };

        match client.objects().get(container_name, &name).send().await {
            Ok(obj) => match obj.bytes().await {
                Ok(full_data) => {
                    let start_idx = start.min(full_data.len() as u64) as usize;
                    let end_idx = end.min(full_data.len() as u64) as usize;
                    let data_slice = full_data
                        .get(start_idx..end_idx)
                        .unwrap_or_default()
                        .to_vec();

                    debug!(
                        container = container_name,
                        object = name,
                        original_size = full_data.len(),
                        slice_size = data_slice.len(),
                        start_idx = start_idx,
                        end_idx = end_idx,
                        "Retrieved object data slice"
                    );

                    let resource = self.table.push(data_slice)?;
                    Ok(Ok(resource))
                }
                Err(e) => Ok(Err(format!("failed to read object bytes: {}", e))),
            },
            Err(e) => {
                debug!(
                    container = container_name,
                    object = name,
                   error = %e,
                    "Object not found"
                );
                Ok(Err(format!("object '{}' not found: {}", name, e)))
            }
        }
    }

    async fn write_data(
        &mut self,
        container: Resource<String>,
        name: ObjectName,
        data: Resource<OutgoingValueHandle>,
    ) -> wasmtime::Result<Result<(), ContainerError>> {
        let container_name = self.table.get(&container)?.clone();

        debug!(
            container = container_name,
            object = name,
            workload_id = self.id,
            "Initiating write_data for object"
        );

        let Some(_plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err(
                "Cloudflare R2 blobstore plugin not available".to_string()
            ));
        };

        // Store the container and object names - actual writing happens in finish()
        let outgoing_handle = self.table.get_mut(&data)?;
        outgoing_handle.container_name = Some(container_name.clone());
        outgoing_handle.object_name = Some(name.clone());

        debug!(
            container = container_name,
            object = name,
            "write_data setup complete, actual write will happen in finish()"
        );

        Ok(Ok(()))
    }

    async fn list_objects(
        &mut self,
        container: Resource<String>,
    ) -> wasmtime::Result<Result<Resource<StreamObjectNamesHandle>, ContainerError>> {
        let container_name = self.table.get(&container)?;

        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err(
                "Cloudflare R2 blobstore plugin not available".to_string()
            ));
        };

        let client = match plugin.get_or_create_client(self.workload_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(format!("failed to get client: {}", e))),
        };

        debug!(container = container_name, "Listing objects in container");

        match client.objects().list_v2(container_name).send().await {
            Ok(list_result) => {
                let objects: Vec<String> = list_result
                    .contents
                    .into_iter()
                    .map(|obj| obj.key)
                    .collect();

                debug!(
                    container = container_name,
                    count = objects.len(),
                    "Listed objects in container"
                );

                let handle = StreamObjectNamesHandle {
                    objects,
                    position: 0,
                };
                let resource = self.table.push(handle)?;
                Ok(Ok(resource))
            }
            Err(e) => Ok(Err(format!("failed to list objects: {}", e))),
        }
    }

    async fn delete_object(
        &mut self,
        container: Resource<String>,
        name: ObjectName,
    ) -> wasmtime::Result<Result<(), ContainerError>> {
        let container_name = self.table.get(&container)?;

        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err(
                "Cloudflare R2 blobstore plugin not available".to_string()
            ));
        };

        let client = match plugin.get_or_create_client(self.workload_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(format!("failed to get client: {}", e))),
        };

        if let Err(e) = client.objects().delete(container_name, &name).send().await {
            return Ok(Err(format!("failed to delete object: {}", e)));
        }

        Ok(Ok(()))
    }

    async fn delete_objects(
        &mut self,
        container: Resource<String>,
        names: Vec<ObjectName>,
    ) -> wasmtime::Result<Result<(), ContainerError>> {
        let container_name = self.table.get(&container)?;

        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err(
                "Cloudflare R2 blobstore plugin not available".to_string()
            ));
        };

        let client = match plugin.get_or_create_client(self.workload_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(format!("failed to get client: {}", e))),
        };

        for name in names {
            if let Err(e) = client.objects().delete(container_name, &name).send().await {
                return Ok(Err(format!("failed to delete object '{}': {}", name, e)));
            }
        }

        Ok(Ok(()))
    }

    async fn has_object(
        &mut self,
        container: Resource<String>,
        name: ObjectName,
    ) -> wasmtime::Result<Result<bool, ContainerError>> {
        let container_name = self.table.get(&container)?;

        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err(
                "Cloudflare R2 blobstore plugin not available".to_string()
            ));
        };

        let client = match plugin.get_or_create_client(self.workload_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(format!("failed to get client: {}", e))),
        };

        // Try to get object metadata to check if it exists
        match client.objects().head(container_name, &name).send().await {
            Ok(_) => Ok(Ok(true)),
            Err(_) => Ok(Ok(false)),
        }
    }

    async fn object_info(
        &mut self,
        container: Resource<String>,
        name: ObjectName,
    ) -> wasmtime::Result<Result<ObjectMetadata, ContainerError>> {
        let container_name = self.table.get(&container)?;

        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err(
                "Cloudflare R2 blobstore plugin not available".to_string()
            ));
        };

        let client = match plugin.get_or_create_client(self.workload_id.as_ref()).await {
            Ok(c) => c,
            Err(e) => return Ok(Err(format!("failed to get client: {}", e))),
        };

        match client.objects().head(container_name, &name).send().await {
            Ok(head_result) => Ok(Ok(ObjectMetadata {
                name: name.clone(),
                container: container_name.clone(),
                created_at: 0, // R2 doesn't provide object creation time via HEAD
                size: head_result.content_length.unwrap_or(0),
            })),
            Err(e) => Ok(Err(format!("object '{}' not found: {}", name, e))),
        }
    }

    async fn clear(
        &mut self,
        _container: Resource<String>,
    ) -> wasmtime::Result<Result<(), ContainerError>> {
        // Not supported - would require listing and deleting all objects
        Ok(Err(
            "clear not supported - would require listing all objects".to_string(),
        ))
    }

    async fn drop(&mut self, rep: Resource<String>) -> wasmtime::Result<()> {
        debug!(
            workload_id = self.id,
            resource_id = ?rep,
            "Dropping container resource"
        );
        self.table.delete(rep)?;
        Ok(())
    }
}

// ============================================================================
// Stream Object Names Implementation
// ============================================================================

impl<'a> bindings::wasi::blobstore::container::HostStreamObjectNames for ActiveCtx<'a> {
    async fn read_stream_object_names(
        &mut self,
        stream: Resource<StreamObjectNamesHandle>,
        len: u64,
    ) -> wasmtime::Result<Result<(Vec<ObjectName>, bool), ContainerError>> {
        let stream_handle = self.table.get_mut(&stream)?;

        let remaining = stream_handle
            .objects
            .len()
            .saturating_sub(stream_handle.position);
        let to_read = (len as usize).min(remaining);

        let mut objects = Vec::new();
        for i in 0..to_read {
            if let Some(obj_name) = stream_handle.objects.get(stream_handle.position + i) {
                objects.push(obj_name.clone());
            }
        }

        stream_handle.position += to_read;
        let is_end = stream_handle.position >= stream_handle.objects.len();

        Ok(Ok((objects, is_end)))
    }

    async fn skip_stream_object_names(
        &mut self,
        stream: Resource<StreamObjectNamesHandle>,
        num: u64,
    ) -> wasmtime::Result<Result<(u64, bool), ContainerError>> {
        let stream_handle = self.table.get_mut(&stream)?;

        let remaining = stream_handle
            .objects
            .len()
            .saturating_sub(stream_handle.position);
        let to_skip = (num as usize).min(remaining);

        stream_handle.position += to_skip;
        let is_end = stream_handle.position >= stream_handle.objects.len();

        Ok(Ok((to_skip as u64, is_end)))
    }

    async fn drop(&mut self, rep: Resource<StreamObjectNamesHandle>) -> wasmtime::Result<()> {
        debug!(
            workload_id = self.id,
            resource_id = ?rep,
            "Dropping StreamObjectNames resource"
        );
        self.table.delete(rep)?;
        Ok(())
    }
}

// ============================================================================
// Types Interface Implementation
// ============================================================================

impl<'a> bindings::wasi::blobstore::types::HostOutgoingValue for ActiveCtx<'a> {
    async fn new_outgoing_value(&mut self) -> wasmtime::Result<Resource<OutgoingValueHandle>> {
        debug!(workload_id = self.id, "Creating new OutgoingValue");

        let handle = OutgoingValueHandle {
            pipe: MemoryOutputPipe::new(10 * 1024 * 1024), // 10MB max
            container_name: None,
            object_name: None,
        };

        debug!(
            workload_id = self.id,
            "Created OutgoingValueHandle with MemoryOutputPipe"
        );

        match self.table.push(handle) {
            Ok(resource) => {
                debug!(
                    workload_id = self.id,
                    resource_id = ?resource,
                    "Successfully pushed OutgoingValueHandle to resource table"
                );
                Ok(resource)
            }
            Err(e) => {
                debug!(
                    workload_id = self.id,
                    error = e.to_string(),
                    "Failed to push OutgoingValueHandle to resource table"
                );
                Err(e.into())
            }
        }
    }

    async fn outgoing_value_write_body(
        &mut self,
        outgoing_value: Resource<OutgoingValueHandle>,
    ) -> wasmtime::Result<Result<Resource<bindings::wasi::io0_2_1::streams::OutputStream>, ()>>
    {
        debug!(workload_id = self.id, "outgoing_value_write_body called");

        let handle = match self.table.get_mut(&outgoing_value) {
            Ok(h) => {
                debug!(
                    workload_id = self.ctx.id,
                    "Successfully retrieved OutgoingValueHandle from table"
                );
                h
            }
            Err(e) => {
                debug!(
                    workload_id = self.id,
                    error = e.to_string(),
                    "Failed to get OutgoingValueHandle from table"
                );
                return Err(e.into());
            }
        };

        debug!(
            workload_id = self.ctx.id,
            "Creating boxed OutputStream from pipe"
        );

        // Return the pipe as the output stream
        let boxed: Box<dyn OutputStream> = Box::new(handle.pipe.clone());

        match self.table.push(boxed) {
            Ok(stream) => {
                debug!(
                    workload_id = self.id,
                    stream_resource_id = ?stream,
                    "Successfully pushed OutputStream to resource table"
                );
                Ok(Ok(stream))
            }
            Err(e) => {
                debug!(
                    workload_id = self.id,
                    error = e.to_string(),
                    "Failed to push OutputStream to resource table"
                );
                Err(e.into())
            }
        }
    }

    async fn finish(
        &mut self,
        outgoing_value: Resource<OutgoingValueHandle>,
    ) -> wasmtime::Result<Result<(), BlobstoreError>> {
        debug!(workload_id = self.id, "finish() called for OutgoingValue");

        let handle = self.table.delete(outgoing_value)?;

        debug!(
            container_name = ?handle.container_name,
            object_name = ?handle.object_name,
            "Retrieved OutgoingValueHandle in finish()"
        );

        // If we have container and object names, perform the actual write
        if let (Some(container_name), Some(object_name)) =
            (&handle.container_name, &handle.object_name)
        {
            let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
                return Ok(Err(
                    "Cloudflare R2 blobstore plugin not available".to_string()
                ));
            };

            // Get the data from the pipe
            let data_bytes = handle.pipe.contents();

            debug!(
                container = container_name,
                object = object_name,
                pipe_data_size = data_bytes.len(),
                workload_id = self.workload_id.to_string(),
                "Retrieved data from pipe in finish()"
            );

            let client = match plugin.get_or_create_client(self.workload_id.as_ref()).await {
                Ok(c) => c,
                Err(e) => return Ok(Err(format!("failed to get client: {}", e))),
            };

            if let Err(e) = client
                .objects()
                .put(container_name, object_name)
                .body_bytes(data_bytes.clone())
                .send()
                .await
            {
                return Ok(Err(format!("failed to upload object: {}", e)));
            }

            debug!(
                container = container_name,
                object = object_name,
                size = data_bytes.len(),
                "Uploaded object to R2"
            );
        } else {
            debug!(
                workload_id = self.id,
                "finish() called without container/object names set"
            );
        }

        Ok(Ok(()))
    }

    async fn drop(&mut self, rep: Resource<OutgoingValueHandle>) -> wasmtime::Result<()> {
        debug!(
            workload_id = self.id,
            resource_id = ?rep,
            "Dropping OutgoingValue resource"
        );
        match self.finish(rep).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

impl<'a> bindings::wasi::blobstore::types::HostIncomingValue for ActiveCtx<'a> {
    async fn incoming_value_consume_sync(
        &mut self,
        incoming_value: Resource<IncomingValueHandle>,
    ) -> wasmtime::Result<Result<Vec<u8>, BlobstoreError>> {
        let data = self.table.delete(incoming_value)?;

        debug!(
            workload_id = self.id,
            data_size = data.len(),
            "incoming_value_consume_sync returning data"
        );

        Ok(Ok(data))
    }

    async fn incoming_value_consume_async(
        &mut self,
        incoming_value: Resource<IncomingValueHandle>,
    ) -> wasmtime::Result<
        Result<Resource<bindings::wasi::blobstore::types::IncomingValueAsyncBody>, BlobstoreError>,
    > {
        let data = self.table.get(&incoming_value)?;

        debug!(
            workload_id = self.id,
            data_size = data.len(),
            "incoming_value_consume_async creating MemoryInputPipe with data"
        );

        let stream: Box<dyn InputStream> = Box::new(MemoryInputPipe::new(data.clone()));
        let stream = self.table.push(stream)?;

        debug!(
            workload_id = self.id,
            "incoming_value_consume_async created stream resource"
        );

        Ok(Ok(stream))
    }

    async fn size(
        &mut self,
        incoming_value: Resource<IncomingValueHandle>,
    ) -> wasmtime::Result<u64> {
        let data = self.table.get(&incoming_value)?;
        Ok(data.len() as u64)
    }

    async fn drop(&mut self, rep: Resource<IncomingValueHandle>) -> wasmtime::Result<()> {
        debug!(
            workload_id = self.id,
            resource_id = ?rep,
            "Dropping IncomingValue resource"
        );
        self.table.delete(rep)?;
        Ok(())
    }
}

// Implement the main types Host trait that combines all resource types
impl<'a> bindings::wasi::blobstore::types::Host for ActiveCtx<'a> {}

// Implement the main container Host trait that combines all resource types
impl<'a> bindings::wasi::blobstore::container::Host for ActiveCtx<'a> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_id() {
        let plugin = CloudflareR2::new();
        assert_eq!(plugin.id(), PLUGIN_ID);
    }

    #[test]
    fn test_world() {
        let plugin = CloudflareR2::new();
        let world = plugin.world();

        assert!(
            world
                .imports
                .iter()
                .any(|i| i.namespace == "wasi" && i.package == "blobstore")
        );
    }

    #[test]
    fn test_default() {
        let plugin = CloudflareR2::default();
        assert_eq!(plugin.id(), PLUGIN_ID);
    }
}
