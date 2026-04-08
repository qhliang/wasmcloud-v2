//! Multi-Backend Blobstore Host Plugin
//!
//! This module implements a wasmCloud host plugin that provides `wasi:blobstore@0.2.0-draft`
//! interfaces using OpenDAL as the unified storage access layer.
//!
//! Supported backends: S3 (including Cloudflare R2), WebDAV, FTP, Filesystem.
//!
//! Backend is selected via `backend` config key (e.g., "s3", "webdav", "ftp", "fs").
//! All other config keys are the YAML interface config are passed directly to OpenDAL.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use opentelemetry::metrics::Counter;
use opendal::{Operator, Scheme};
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

const PLUGIN_ID: &str = "wasi-blobstore-multi-backend";

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

/// Extract blobstore config from interface config.
/// The `backend` key is required (e.g., "s3", "webdav", "ftp", "fs").
/// All other keys are passed directly to OpenDAL as backend-specific config.
fn extract_config(interface: &WitInterface) -> anyhow::Result<Operator> {
    let backend = interface
        .config
        .get("backend")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing required config: 'backend'"))?;

    let scheme = Scheme::from_str(&backend)
        .map_err(|e| anyhow::anyhow!("unknown backend '{backend}': {e}"))?;

    let iter = interface
        .config
        .iter()
        .filter(|(k, _)| k.as_str() != "backend")
        .map(|(k, v)| (k.clone(), v.clone()));

    let op = Operator::via_iter(scheme, iter)
        .map_err(|e| anyhow::anyhow!("failed to create OpenDAL operator for backend '{backend}': {e}"))?;

    debug!(backend = backend, "Created OpenDAL operator");
    Ok(op)
}

/// Multi-backend blobstore plugin
#[derive(Clone, Default)]
pub struct CloudflareR2 {
    /// Per-workload OpenDAL operators
    operators: Arc<RwLock<HashMap<String, Operator>>>,
    metrics: Arc<BlobstoreMetrics>,
}

struct BlobstoreMetrics {
    operations_total: Counter<u64>,
}

impl Default for BlobstoreMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl BlobstoreMetrics {
    fn new() -> Self {
        let meter = opentelemetry::global::meter("wasi-blobstore-multi-backend");
        let operations_total = meter
            .u64_counter("wasi_blobstore_operations_total")
            .with_description("Total number of blobstore operations")
            .build();
        Self { operations_total }
    }
}

impl CloudflareR2 {
    /// Create a new blobstore plugin.
    /// Backend is configured per-workload via interface config.
    pub fn new() -> Self {
        let metrics = BlobstoreMetrics::new();
        Self {
            operators: Arc::new(RwLock::new(HashMap::new())),
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

    async fn get_or_create_operator(
        &self,
        workload_id: &str,
        interface: &WitInterface,
    ) -> anyhow::Result<Operator> {
        // Check if operator already exists
        {
            let operators = self.operators.read().await;
            if let Some(op) = operators.get(workload_id) {
                return Ok(op.clone());
            }
        }

        let op = extract_config(interface)?;

        // Cache the operator
        {
            let mut operators = self.operators.write().await;
            operators.insert(workload_id.to_string(), op.clone());
        }

        Ok(op)
    }

    /// Get an existing operator for a workload
    async fn get_operator(&self, workload_id: &str) -> anyhow::Result<Operator> {
        let operators = self.operators.read().await;
        operators
            .get(workload_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("No operator found for workload '{}'", workload_id))
    }
}

#[async_trait]
impl HostPlugin for CloudflareR2 {
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: HashSet::from([WitInterface::from(
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
        let blobstore_interface = interfaces
            .iter()
            .find(|i| i.namespace == "wasi" && i.package == "blobstore");

        let Some(interface) = blobstore_interface else {
            tracing::warn!(
                "Blobstore plugin requested for non-wasi:blobstore interface(s): {:?}",
                interfaces
            );
            return Ok(());
        };

        let workload_id = component_handle.workload_id().to_string();

        // Validate config by creating operator
        let backend = interface.config.get("backend").cloned().unwrap_or_default();
        if backend.is_empty() {
            tracing::error!(
                workload_id = %workload_id,
                "Missing required config: 'backend' for blobstore plugin. Supported: s3, webdav, ftp, fs"
            );
            return Ok(());
        }

        match self.get_or_create_operator(&workload_id, interface).await {
            Ok(_) => {
                debug!(
                    workload_id = %workload_id,
                    backend = backend,
                    "Configured blobstore backend for workload"
                );
            }
            Err(e) => {
                tracing::error!(
                    workload_id = %workload_id,
                    backend = backend,
                    error = %e,
                    "Failed to configure blobstore backend"
                );
                return Ok(());
            }
        }

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

        debug!("Blobstore plugin bound to workload '{workload_id}'");

        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        {
            let mut operators = self.operators.write().await;
            operators.remove(workload_id);
        }

        debug!("Blobstore plugin unbound from workload '{workload_id}'");
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
            return Ok(Err("Blobstore plugin not available".to_string()));
        };
        plugin.record_operation("create_container");

        debug!(
            workload_id = self.workload_id.as_ref(),
            container_name = name,
            "Creating container"
        );

        let op = match plugin.get_operator(self.workload_id.as_ref()).await {
            Ok(o) => o,
            Err(e) => return Ok(Err(format!("failed to get operator: {e}"))),
        };

        // Create the container as a directory
        let path = format!("{name}/");
        if let Err(e) = op.create_dir(&path).await {
            debug!(error = %e, "Failed to create container directory");
            // Some backends don't support explicit directory creation; treat as success
        }

        let resource = self.table.push(name)?;
        Ok(Ok(resource))
    }

    async fn get_container(
        &mut self,
        name: ContainerName,
    ) -> wasmtime::Result<Result<Resource<String>, BlobstoreError>> {
        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };
        plugin.record_operation("get_container");

        let resource = self.table.push(name)?;
        Ok(Ok(resource))
    }

    async fn container_exists(
        &mut self,
        name: ContainerName,
    ) -> wasmtime::Result<Result<bool, BlobstoreError>> {
        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };
        plugin.record_operation("container_exists");

        let op = match plugin.get_operator(self.workload_id.as_ref()).await {
            Ok(o) => o,
            Err(e) => return Ok(Err(format!("failed to get operator: {e}"))),
        };

        let path = format!("{name}/");
        match op.exists(&path).await {
            Ok(exists) => Ok(Ok(exists)),
            Err(_) => Ok(Ok(false)),
        }
    }

    async fn delete_container(
        &mut self,
        name: ContainerName,
    ) -> wasmtime::Result<Result<(), BlobstoreError>> {
        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };
        plugin.record_operation("delete_container");

        debug!(
            workload_id = self.workload_id.as_ref(),
            container_name = name,
            "Deleting container"
        );

        let op = match plugin.get_operator(self.workload_id.as_ref()).await {
            Ok(o) => o,
            Err(e) => return Ok(Err(format!("failed to get operator: {e}"))),
        };

        // Remove all objects under the container prefix
        let path = format!("{name}/");
        if let Err(e) = op.remove_all(&path).await {
            debug!(error = %e, "Failed to delete container");
        }

        Ok(Ok(()))
    }

    async fn copy_object(
        &mut self,
        src: ObjectId,
        dest: ObjectId,
    ) -> wasmtime::Result<Result<(), BlobstoreError>> {
        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };
        plugin.record_operation("copy_object");

        let op = match plugin.get_operator(self.workload_id.as_ref()).await {
            Ok(o) => o,
            Err(e) => return Ok(Err(format!("failed to get operator: {e}"))),
        };

        let src_path = format!("{}/{}", src.container, src.object);
        let dest_path = format!("{}/{}", dest.container, dest.object);

        if let Err(e) = op.copy(&src_path, &dest_path).await {
            return Ok(Err(format!("failed to copy object: {e}")));
        }

        Ok(Ok(()))
    }

    async fn move_object(
        &mut self,
        src: ObjectId,
        dest: ObjectId,
    ) -> wasmtime::Result<Result<(), BlobstoreError>> {
        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };
        plugin.record_operation("move_object");

        let op = match plugin.get_operator(self.workload_id.as_ref()).await {
            Ok(o) => o,
            Err(e) => return Ok(Err(format!("failed to get operator: {e}"))),
        };

        let src_path = format!("{}/{}", src.container, src.object);
        let dest_path = format!("{}/{}", dest.container, dest.object);

        if let Err(e) = op.rename(&src_path, &dest_path).await {
            return Ok(Err(format!("failed to move object: {e}")));
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
            created_at: 0,
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
            "Getting object data"
        );

        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };

        let op = match plugin.get_operator(self.workload_id.as_ref()).await {
            Ok(o) => o,
            Err(e) => return Ok(Err(format!("failed to get operator: {e}"))),
        };

        let path = format!("{container_name}/{name}");

        match op.read(&path).await {
            Ok(full_data) => {
                let data = full_data.to_vec();
                let start_idx = start.min(data.len() as u64) as usize;
                let end_idx = end.min(data.len() as u64) as usize;
                let data_slice = data.get(start_idx..end_idx).unwrap_or_default().to_vec();

                debug!(
                    container = container_name,
                    object = name,
                    original_size = data.len(),
                    slice_size = data_slice.len(),
                    "Retrieved object data slice"
                );

                let resource = self.table.push(data_slice)?;
                Ok(Ok(resource))
            }
            Err(e) => {
                debug!(
                    container = container_name,
                    object = name,
                    error = %e,
                    "Object not found"
                );
                Ok(Err(format!("object '{name}' not found: {e}")))
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

        // Store the container and object names for actual writing in finish()
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
            return Ok(Err("Blobstore plugin not available".to_string()));
        };

        let op = match plugin.get_operator(self.workload_id.as_ref()).await {
            Ok(o) => o,
            Err(e) => return Ok(Err(format!("failed to get operator: {e}"))),
        };

        debug!(container = container_name, "Listing objects in container");

        let path = format!("{container_name}/");
        match op.list(&path).await {
            Ok(raw_entries) => {
                let names: Vec<String> = raw_entries
                    .into_iter()
                    .map(|e| {
                        let name = e.name().to_string();
                        name.trim_end_matches('/').to_string()
                    })
                    .filter(|n| !n.is_empty())
                    .collect();

                debug!(
                    container = container_name,
                    count = names.len(),
                    "Listed objects in container"
                );

                let handle = StreamObjectNamesHandle {
                    objects: names,
                    position: 0,
                };
                let resource = self.table.push(handle)?;
                Ok(Ok(resource))
            }
            Err(e) => Ok(Err(format!("failed to list objects: {e}"))),
        }
    }

    async fn delete_object(
        &mut self,
        container: Resource<String>,
        name: ObjectName,
    ) -> wasmtime::Result<Result<(), ContainerError>> {
        let container_name = self.table.get(&container)?;

        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };

        let op = match plugin.get_operator(self.workload_id.as_ref()).await {
            Ok(o) => o,
            Err(e) => return Ok(Err(format!("failed to get operator: {e}"))),
        };

        let path = format!("{container_name}/{name}");
        if let Err(e) = op.delete(&path).await {
            return Ok(Err(format!("failed to delete object: {e}")));
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
            return Ok(Err("Blobstore plugin not available".to_string()));
        };

        let op = match plugin.get_operator(self.workload_id.as_ref()).await {
            Ok(o) => o,
            Err(e) => return Ok(Err(format!("failed to get operator: {e}"))),
        };

        for name in &names {
            let path = format!("{container_name}/{name}");
            if let Err(e) = op.delete(&path).await {
                return Ok(Err(format!("failed to delete object '{name}': {e}")));
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
            return Ok(Err("Blobstore plugin not available".to_string()));
        };

        let op = match plugin.get_operator(self.workload_id.as_ref()).await {
            Ok(o) => o,
            Err(e) => return Ok(Err(format!("failed to get operator: {e}"))),
        };

        let path = format!("{container_name}/{name}");
        match op.exists(&path).await {
            Ok(exists) => Ok(Ok(exists)),
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
            return Ok(Err("Blobstore plugin not available".to_string()));
        };

        let op = match plugin.get_operator(self.workload_id.as_ref()).await {
            Ok(o) => o,
            Err(e) => return Ok(Err(format!("failed to get operator: {e}"))),
        };

        let path = format!("{container_name}/{name}");
        match op.stat(&path).await {
            Ok(meta) => Ok(Ok(ObjectMetadata {
                name: name.clone(),
                container: container_name.clone(),
                created_at: meta.last_modified().map_or(0, |ts| ts.timestamp_millis() as u64),
                size: meta.content_length(),
            })),
            Err(e) => Ok(Err(format!("object '{name}' not found: {e}"))),
        }
    }

    async fn clear(
        &mut self,
        container: Resource<String>,
    ) -> wasmtime::Result<Result<(), ContainerError>> {
        let container_name = self.table.get(&container)?;

        let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };

        let op = match plugin.get_operator(self.workload_id.as_ref()).await {
            Ok(o) => o,
            Err(e) => return Ok(Err(format!("failed to get operator: {e}"))),
        };

        let path = format!("{container_name}/");
        if let Err(e) = op.remove_all(&path).await {
            return Ok(Err(format!("failed to clear container: {e}")));
        }

        Ok(Ok(()))
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
            Ok(h) => h,
            Err(e) => {
                debug!(
                    workload_id = self.id,
                    error = e.to_string(),
                    "Failed to get OutgoingValueHandle from table"
                );
                return Err(e.into());
            }
        };

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

        if let (Some(container_name), Some(object_name)) =
            (&handle.container_name, &handle.object_name)
        {
            let Some(plugin) = self.get_plugin::<CloudflareR2>(PLUGIN_ID) else {
                return Ok(Err("Blobstore plugin not available".to_string()));
            };

            let data_bytes = handle.pipe.contents().to_vec();
            let data_len = data_bytes.len();

            debug!(
                container = container_name,
                object = object_name,
                pipe_data_size = data_len,
                workload_id = self.workload_id.to_string(),
                "Retrieved data from pipe in finish()"
            );

            let op = match plugin.get_operator(self.workload_id.as_ref()).await {
                Ok(o) => o,
                Err(e) => return Ok(Err(format!("failed to get operator: {e}"))),
            };

            let path = format!("{container_name}/{object_name}");
            if let Err(e) = op.write(&path, data_bytes).await {
                return Ok(Err(format!("failed to upload object: {e}")));
            }

            debug!(
                container = container_name,
                object = object_name,
                size = data_len,
                "Uploaded object"
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

impl<'a> bindings::wasi::blobstore::types::Host for ActiveCtx<'a> {}
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
