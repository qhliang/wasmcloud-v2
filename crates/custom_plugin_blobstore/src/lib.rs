//! Multi-Backend Blobstore Host Plugin
//!
//! This module implements a wasmCloud host plugin that provides `wasi:blobstore@0.2.0-draft`
//! interfaces using OpenDAL or NATS JetStream Object Store as the storage backend.
//!
//! Supported backends: Memory (default), S3 (including Cloudflare R2), WebDAV, FTP, Filesystem,
//! NATS JetStream Object Store.
//!
//! Backend is selected via `backend` config key (e.g., "memory", "s3", "webdav", "ftp", "fs", "nats").
//! If `backend` is not specified, "memory" is used by default.
//! All other config keys from the YAML interface config are passed directly to OpenDAL.
//! For NATS backend, use `nats_url` (default: "nats://127.0.0.1:4222") to configure the connection.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use opendal::{Operator, Scheme};
use opentelemetry::metrics::Counter;
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;
use tracing::debug;
use wasmtime::component::Resource;
use wasmtime_wasi::p2::{
    InputStream, OutputStream,
    pipe::{MemoryInputPipe, MemoryOutputPipe},
};

use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::WorkloadItem;
use wash_runtime::plugin::{HostPlugin, WorkloadTracker};
use wash_runtime::wit::{WitInterface, WitWorld};

use custom_plugin_nats_utils::build_nats_connect_options;

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

/// Backend engine for blobstore operations
enum BlobEngine {
    /// OpenDAL operator (memory, s3, webdav, ftp, fs)
    OpenDal(Operator),
    /// NATS JetStream Object Store
    Nats(NatsBlobBackend),
}

impl BlobEngine {
    fn clone_engine(&self) -> BlobEngine {
        match self {
            BlobEngine::OpenDal(op) => BlobEngine::OpenDal(op.clone()),
            BlobEngine::Nats(nats) => BlobEngine::Nats(nats.clone()),
        }
    }
}

/// NATS JetStream Object Store backend
#[derive(Clone)]
struct NatsBlobBackend {
    context: Arc<async_nats::jetstream::Context>,
}

impl NatsBlobBackend {
    async fn new(config: &HashMap<String, String>) -> anyhow::Result<Self> {
        let nats_url = config
            .get("nats_url")
            .cloned()
            .unwrap_or_else(|| "nats://127.0.0.1:4222".to_string());

        let opts = build_nats_connect_options(config)?;
        let client = opts
            .connect(&nats_url)
            .await
            .map_err(|e| anyhow::anyhow!("failed to connect to NATS: {e}"))?;
        let context = async_nats::jetstream::new(client);
        Ok(Self {
            context: Arc::new(context),
        })
    }

    async fn create_container(&self, name: &str) -> anyhow::Result<()> {
        self.context
            .create_object_store(async_nats::jetstream::object_store::Config {
                bucket: name.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow::anyhow!("failed to create object store '{name}': {e}"))?;
        Ok(())
    }

    async fn container_exists(&self, name: &str) -> anyhow::Result<bool> {
        match self.context.get_object_store(name.to_string()).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn delete_container(&self, name: &str) -> anyhow::Result<()> {
        self.context
            .delete_object_store(name.to_string())
            .await
            .map_err(|e| anyhow::anyhow!("failed to delete object store '{name}': {e}"))?;
        Ok(())
    }

    async fn read(&self, container: &str, object: &str) -> anyhow::Result<Vec<u8>> {
        let store = self
            .context
            .get_object_store(container.to_string())
            .await
            .map_err(|e| anyhow::anyhow!("failed to get object store '{container}': {e}"))?;
        let mut obj = store
            .get(object)
            .await
            .map_err(|e| anyhow::anyhow!("failed to get object '{object}': {e}"))?;
        let mut buf = Vec::new();
        obj.read_to_end(&mut buf)
            .await
            .map_err(|e| anyhow::anyhow!("failed to read object '{object}': {e}"))?;
        Ok(buf)
    }

    async fn write(&self, container: &str, object: &str, data: Vec<u8>) -> anyhow::Result<()> {
        let store = self
            .context
            .get_object_store(container.to_string())
            .await
            .map_err(|e| anyhow::anyhow!("failed to get object store '{container}': {e}"))?;
        let mut cursor = std::io::Cursor::new(data);
        store
            .put(object, &mut cursor)
            .await
            .map_err(|e| anyhow::anyhow!("failed to put object '{object}': {e}"))?;
        Ok(())
    }

    async fn delete(&self, container: &str, object: &str) -> anyhow::Result<()> {
        let store = self
            .context
            .get_object_store(container.to_string())
            .await
            .map_err(|e| anyhow::anyhow!("failed to get object store '{container}': {e}"))?;
        store
            .delete(object)
            .await
            .map_err(|e| anyhow::anyhow!("failed to delete object '{object}': {e}"))?;
        Ok(())
    }

    async fn list(&self, container: &str) -> anyhow::Result<Vec<String>> {
        let store = self
            .context
            .get_object_store(container.to_string())
            .await
            .map_err(|e| anyhow::anyhow!("failed to get object store '{container}': {e}"))?;
        let mut list_stream = store
            .list()
            .await
            .map_err(|e| anyhow::anyhow!("failed to list objects in '{container}': {e}"))?;
        let mut names = Vec::new();
        while let Some(item) = list_stream.next().await {
            match item {
                Ok(info) => names.push(info.name),
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "failed to list objects in '{container}': {e}"
                    ));
                }
            }
        }
        Ok(names)
    }

    async fn exists(&self, container: &str, object: &str) -> anyhow::Result<bool> {
        let store = self
            .context
            .get_object_store(container.to_string())
            .await
            .map_err(|e| anyhow::anyhow!("failed to get object store '{container}': {e}"))?;
        match store.info(object).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn stat(&self, container: &str, object: &str) -> anyhow::Result<(u64, u64)> {
        let store = self
            .context
            .get_object_store(container.to_string())
            .await
            .map_err(|e| anyhow::anyhow!("failed to get object store '{container}': {e}"))?;
        let info = store
            .info(object)
            .await
            .map_err(|e| anyhow::anyhow!("failed to get object info '{object}': {e}"))?;
        Ok((info.size as u64, 0))
    }

    async fn copy(
        &self,
        src_container: &str,
        src_object: &str,
        dest_container: &str,
        dest_object: &str,
    ) -> anyhow::Result<()> {
        let src_store = self
            .context
            .get_object_store(src_container.to_string())
            .await
            .map_err(|e| {
                anyhow::anyhow!("failed to get source object store '{src_container}': {e}")
            })?;
        let dest_store = self
            .context
            .get_object_store(dest_container.to_string())
            .await
            .map_err(|e| {
                anyhow::anyhow!("failed to get destination object store '{dest_container}': {e}")
            })?;
        let mut obj = src_store
            .get(src_object)
            .await
            .map_err(|e| anyhow::anyhow!("failed to get source object '{src_object}': {e}"))?;
        dest_store.put(dest_object, &mut obj).await.map_err(|e| {
            anyhow::anyhow!("failed to put object to destination '{dest_object}': {e}")
        })?;
        Ok(())
    }

    async fn rename(
        &self,
        src_container: &str,
        src_object: &str,
        dest_container: &str,
        dest_object: &str,
    ) -> anyhow::Result<()> {
        self.copy(src_container, src_object, dest_container, dest_object)
            .await?;
        let src_store = self
            .context
            .get_object_store(src_container.to_string())
            .await
            .map_err(|e| {
                anyhow::anyhow!("failed to get source object store '{src_container}': {e}")
            })?;
        src_store
            .delete(src_object)
            .await
            .map_err(|e| anyhow::anyhow!("failed to delete source object '{src_object}': {e}"))?;
        Ok(())
    }

    async fn clear(&self, container: &str) -> anyhow::Result<()> {
        let store = self
            .context
            .get_object_store(container.to_string())
            .await
            .map_err(|e| anyhow::anyhow!("failed to get object store '{container}': {e}"))?;
        let mut list_stream = store
            .list()
            .await
            .map_err(|e| anyhow::anyhow!("failed to list objects in '{container}': {e}"))?;
        while let Some(item) = list_stream.next().await {
            match item {
                Ok(info) => {
                    if let Err(e) = store.delete(&info.name).await {
                        return Err(anyhow::anyhow!(
                            "failed to delete object '{}' in '{container}': {e}",
                            info.name
                        ));
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "failed to list objects in '{container}': {e}"
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Per-component data.
struct ComponentData {
    /// Static interface config from wasmcloud config
    interface_config: HashMap<String, String>,
    /// Cached backend engine
    engine: Option<BlobEngine>,
}

/// Multi-backend blobstore plugin
#[derive(Clone)]
pub struct CustomBlobstore {
    /// Per-component state tracker
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
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

impl Default for CustomBlobstore {
    fn default() -> Self {
        Self::new()
    }
}

impl CustomBlobstore {
    /// Create a new blobstore plugin.
    /// Backend is configured per-workload via interface config.
    pub fn new() -> Self {
        let metrics = BlobstoreMetrics::new();
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

    /// Get an existing engine for a component, creating one if needed.
    async fn get_engine(&self, component_id: &str) -> anyhow::Result<BlobEngine> {
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
                        "No blobstore config found for component '{}'",
                        component_id
                    ));
                }
            }
        };

        let backend = interface_config
            .get("backend")
            .cloned()
            .unwrap_or_else(|| "memory".to_string());

        let engine = match backend.as_str() {
            "nats" => {
                let nats = NatsBlobBackend::new(&interface_config).await?;
                BlobEngine::Nats(nats)
            }
            _ => {
                // OpenDAL backends: memory, s3, webdav, ftp, fs, etc.
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
                BlobEngine::OpenDal(op)
            }
        };

        // Cache the engine
        {
            let mut lock = self.tracker.write().await;
            if let Some(data) = lock.get_component_data_mut(component_id) {
                data.engine = Some(engine.clone_engine());
            }
        }

        Ok(engine)
    }
}

#[async_trait]
impl HostPlugin for CustomBlobstore {
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
        item: &mut WorkloadItem<'a>,
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

        let interface_config = interface.config.clone();

        let linker = item.linker();
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

        let WorkloadItem::Component(component_handle) = item else {
            return Ok(());
        };

        debug!(
            component_id = component_handle.id(),
            "Blobstore plugin bound to component"
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
        _interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        self.tracker
            .write()
            .await
            .remove_workload_with_cleanup(workload_id, |_| async {}, |_| async {})
            .await;
        debug!(workload_id = %workload_id, "Blobstore plugin unbound");
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
        let Some(plugin) = self.get_plugin::<CustomBlobstore>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };
        plugin.record_operation("create_container");

        debug!(
            workload_id = self.workload_id.as_ref(),
            container_name = name,
            "Creating container"
        );

        let engine = match plugin.get_engine(&self.component_id).await {
            Ok(e) => e,
            Err(e) => return Ok(Err(format!("failed to get engine: {e}"))),
        };

        match engine {
            BlobEngine::OpenDal(op) => {
                let path = format!("{name}/");
                if let Err(e) = op.create_dir(&path).await {
                    debug!(error = %e, "Failed to create container directory");
                }
            }
            BlobEngine::Nats(nats) => {
                if let Err(e) = nats.create_container(&name).await {
                    return Ok(Err(format!("failed to create container: {e}")));
                }
            }
        }

        let resource = self.table.push(name)?;
        Ok(Ok(resource))
    }

    async fn get_container(
        &mut self,
        name: ContainerName,
    ) -> wasmtime::Result<Result<Resource<String>, BlobstoreError>> {
        let Some(plugin) = self.get_plugin::<CustomBlobstore>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };
        plugin.record_operation("get_container");

        let engine = match plugin.get_engine(&self.component_id).await {
            Ok(e) => e,
            Err(e) => return Ok(Err(format!("failed to get engine: {e}"))),
        };

        // For NATS backend, ensure the object store (bucket) exists
        if let BlobEngine::Nats(nats) = &engine
            && !nats.container_exists(&name).await.unwrap_or(false)
            && let Err(e) = nats.create_container(&name).await
        {
            return Ok(Err(format!("failed to create container: {e}")));
        }

        let resource = self.table.push(name)?;
        Ok(Ok(resource))
    }

    async fn container_exists(
        &mut self,
        name: ContainerName,
    ) -> wasmtime::Result<Result<bool, BlobstoreError>> {
        let Some(plugin) = self.get_plugin::<CustomBlobstore>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };
        plugin.record_operation("container_exists");

        let engine = match plugin.get_engine(&self.component_id).await {
            Ok(e) => e,
            Err(e) => return Ok(Err(format!("failed to get engine: {e}"))),
        };

        match engine {
            BlobEngine::OpenDal(op) => {
                let path = format!("{name}/");
                match op.exists(&path).await {
                    Ok(exists) => Ok(Ok(exists)),
                    Err(_) => Ok(Ok(false)),
                }
            }
            BlobEngine::Nats(nats) => match nats.container_exists(&name).await {
                Ok(exists) => Ok(Ok(exists)),
                Err(e) => Ok(Err(format!("failed to check container: {e}"))),
            },
        }
    }

    async fn delete_container(
        &mut self,
        name: ContainerName,
    ) -> wasmtime::Result<Result<(), BlobstoreError>> {
        let Some(plugin) = self.get_plugin::<CustomBlobstore>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };
        plugin.record_operation("delete_container");

        debug!(
            workload_id = self.workload_id.as_ref(),
            container_name = name,
            "Deleting container"
        );

        let engine = match plugin.get_engine(&self.component_id).await {
            Ok(e) => e,
            Err(e) => return Ok(Err(format!("failed to get engine: {e}"))),
        };

        match engine {
            BlobEngine::OpenDal(op) => {
                let path = format!("{name}/");
                if let Err(e) = op.remove_all(&path).await {
                    debug!(error = %e, "Failed to delete container");
                }
            }
            BlobEngine::Nats(nats) => {
                if let Err(e) = nats.delete_container(&name).await {
                    return Ok(Err(format!("failed to delete container: {e}")));
                }
            }
        }

        Ok(Ok(()))
    }

    async fn copy_object(
        &mut self,
        src: ObjectId,
        dest: ObjectId,
    ) -> wasmtime::Result<Result<(), BlobstoreError>> {
        let Some(plugin) = self.get_plugin::<CustomBlobstore>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };
        plugin.record_operation("copy_object");

        let engine = match plugin.get_engine(&self.component_id).await {
            Ok(e) => e,
            Err(e) => return Ok(Err(format!("failed to get engine: {e}"))),
        };

        match engine {
            BlobEngine::OpenDal(op) => {
                let src_path = format!("{}/{}", src.container, src.object);
                let dest_path = format!("{}/{}", dest.container, dest.object);
                if let Err(e) = op.copy(&src_path, &dest_path).await {
                    return Ok(Err(format!("failed to copy object: {e}")));
                }
            }
            BlobEngine::Nats(nats) => {
                if let Err(e) = nats
                    .copy(&src.container, &src.object, &dest.container, &dest.object)
                    .await
                {
                    return Ok(Err(format!("failed to copy object: {e}")));
                }
            }
        }

        Ok(Ok(()))
    }

    async fn move_object(
        &mut self,
        src: ObjectId,
        dest: ObjectId,
    ) -> wasmtime::Result<Result<(), BlobstoreError>> {
        let Some(plugin) = self.get_plugin::<CustomBlobstore>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };
        plugin.record_operation("move_object");

        let engine = match plugin.get_engine(&self.component_id).await {
            Ok(e) => e,
            Err(e) => return Ok(Err(format!("failed to get engine: {e}"))),
        };

        match engine {
            BlobEngine::OpenDal(op) => {
                let src_path = format!("{}/{}", src.container, src.object);
                let dest_path = format!("{}/{}", dest.container, dest.object);
                if let Err(e) = op.rename(&src_path, &dest_path).await {
                    return Ok(Err(format!("failed to move object: {e}")));
                }
            }
            BlobEngine::Nats(nats) => {
                if let Err(e) = nats
                    .rename(&src.container, &src.object, &dest.container, &dest.object)
                    .await
                {
                    return Ok(Err(format!("failed to move object: {e}")));
                }
            }
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

        let Some(plugin) = self.get_plugin::<CustomBlobstore>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };

        let engine = match plugin.get_engine(&self.component_id).await {
            Ok(e) => e,
            Err(e) => return Ok(Err(format!("failed to get engine: {e}"))),
        };

        let full_data = match engine {
            BlobEngine::OpenDal(op) => {
                let path = format!("{container_name}/{name}");
                match op.read(&path).await {
                    Ok(data) => data.to_vec(),
                    Err(e) => {
                        debug!(error = %e, "Object not found");
                        return Ok(Err(format!("object '{name}' not found: {e}")));
                    }
                }
            }
            BlobEngine::Nats(nats) => match nats.read(container_name, &name).await {
                Ok(data) => data,
                Err(e) => {
                    debug!(error = %e, "Object not found");
                    return Ok(Err(format!("object '{name}' not found: {e}")));
                }
            },
        };

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
            "Retrieved object data slice"
        );

        let resource = self.table.push(data_slice)?;
        Ok(Ok(resource))
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

        let Some(plugin) = self.get_plugin::<CustomBlobstore>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };

        let engine = match plugin.get_engine(&self.component_id).await {
            Ok(e) => e,
            Err(e) => return Ok(Err(format!("failed to get engine: {e}"))),
        };

        debug!(container = container_name, "Listing objects in container");

        let names = match engine {
            BlobEngine::OpenDal(op) => {
                let path = format!("{container_name}/");
                match op.list(&path).await {
                    Ok(raw_entries) => raw_entries
                        .into_iter()
                        .map(|e| {
                            let name = e.name().to_string();
                            name.trim_end_matches('/').to_string()
                        })
                        .filter(|n| !n.is_empty())
                        .collect(),
                    Err(e) => return Ok(Err(format!("failed to list objects: {e}"))),
                }
            }
            BlobEngine::Nats(nats) => match nats.list(container_name).await {
                Ok(names) => names,
                Err(e) => return Ok(Err(format!("failed to list objects: {e}"))),
            },
        };

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

    async fn delete_object(
        &mut self,
        container: Resource<String>,
        name: ObjectName,
    ) -> wasmtime::Result<Result<(), ContainerError>> {
        let container_name = self.table.get(&container)?;

        let Some(plugin) = self.get_plugin::<CustomBlobstore>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };

        let engine = match plugin.get_engine(&self.component_id).await {
            Ok(e) => e,
            Err(e) => return Ok(Err(format!("failed to get engine: {e}"))),
        };

        match engine {
            BlobEngine::OpenDal(op) => {
                let path = format!("{container_name}/{name}");
                if let Err(e) = op.delete(&path).await {
                    return Ok(Err(format!("failed to delete object: {e}")));
                }
            }
            BlobEngine::Nats(nats) => {
                if let Err(e) = nats.delete(container_name, &name).await {
                    return Ok(Err(format!("failed to delete object: {e}")));
                }
            }
        }

        Ok(Ok(()))
    }

    async fn delete_objects(
        &mut self,
        container: Resource<String>,
        names: Vec<ObjectName>,
    ) -> wasmtime::Result<Result<(), ContainerError>> {
        let container_name = self.table.get(&container)?;

        let Some(plugin) = self.get_plugin::<CustomBlobstore>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };

        let engine = match plugin.get_engine(&self.component_id).await {
            Ok(e) => e,
            Err(e) => return Ok(Err(format!("failed to get engine: {e}"))),
        };

        for name in &names {
            match &engine {
                BlobEngine::OpenDal(op) => {
                    let path = format!("{container_name}/{name}");
                    if let Err(e) = op.delete(&path).await {
                        return Ok(Err(format!("failed to delete object '{name}': {e}")));
                    }
                }
                BlobEngine::Nats(nats) => {
                    if let Err(e) = nats.delete(container_name, name).await {
                        return Ok(Err(format!("failed to delete object '{name}': {e}")));
                    }
                }
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

        let Some(plugin) = self.get_plugin::<CustomBlobstore>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };

        let engine = match plugin.get_engine(&self.component_id).await {
            Ok(e) => e,
            Err(e) => return Ok(Err(format!("failed to get engine: {e}"))),
        };

        match engine {
            BlobEngine::OpenDal(op) => {
                let path = format!("{container_name}/{name}");
                match op.exists(&path).await {
                    Ok(exists) => Ok(Ok(exists)),
                    Err(_) => Ok(Ok(false)),
                }
            }
            BlobEngine::Nats(nats) => match nats.exists(container_name, &name).await {
                Ok(exists) => Ok(Ok(exists)),
                Err(e) => Ok(Err(format!("failed to check object: {e}"))),
            },
        }
    }

    async fn object_info(
        &mut self,
        container: Resource<String>,
        name: ObjectName,
    ) -> wasmtime::Result<Result<ObjectMetadata, ContainerError>> {
        let container_name = self.table.get(&container)?;

        let Some(plugin) = self.get_plugin::<CustomBlobstore>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };

        let engine = match plugin.get_engine(&self.component_id).await {
            Ok(e) => e,
            Err(e) => return Ok(Err(format!("failed to get engine: {e}"))),
        };

        match engine {
            BlobEngine::OpenDal(op) => {
                let path = format!("{container_name}/{name}");
                match op.stat(&path).await {
                    Ok(meta) => Ok(Ok(ObjectMetadata {
                        name: name.clone(),
                        container: container_name.clone(),
                        created_at: meta
                            .last_modified()
                            .map_or(0, |ts| ts.timestamp_millis() as u64),
                        size: meta.content_length(),
                    })),
                    Err(e) => Ok(Err(format!("object '{name}' not found: {e}"))),
                }
            }
            BlobEngine::Nats(nats) => match nats.stat(container_name, &name).await {
                Ok((size, created_at)) => Ok(Ok(ObjectMetadata {
                    name: name.clone(),
                    container: container_name.clone(),
                    created_at,
                    size,
                })),
                Err(e) => Ok(Err(format!("object '{name}' not found: {e}"))),
            },
        }
    }

    async fn clear(
        &mut self,
        container: Resource<String>,
    ) -> wasmtime::Result<Result<(), ContainerError>> {
        let container_name = self.table.get(&container)?;

        let Some(plugin) = self.get_plugin::<CustomBlobstore>(PLUGIN_ID) else {
            return Ok(Err("Blobstore plugin not available".to_string()));
        };

        let engine = match plugin.get_engine(&self.component_id).await {
            Ok(e) => e,
            Err(e) => return Ok(Err(format!("failed to get engine: {e}"))),
        };

        match engine {
            BlobEngine::OpenDal(op) => {
                let path = format!("{container_name}/");
                if let Err(e) = op.remove_all(&path).await {
                    return Ok(Err(format!("failed to clear container: {e}")));
                }
            }
            BlobEngine::Nats(nats) => {
                if let Err(e) = nats.clear(container_name).await {
                    return Ok(Err(format!("failed to clear container: {e}")));
                }
            }
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
            let Some(plugin) = self.get_plugin::<CustomBlobstore>(PLUGIN_ID) else {
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

            let engine = match plugin.get_engine(&self.component_id).await {
                Ok(e) => e,
                Err(e) => return Ok(Err(format!("failed to get engine: {e}"))),
            };

            match engine {
                BlobEngine::OpenDal(op) => {
                    let path = format!("{container_name}/{object_name}");
                    if let Err(e) = op.write(&path, data_bytes).await {
                        return Ok(Err(format!("failed to upload object: {e}")));
                    }
                }
                BlobEngine::Nats(nats) => {
                    if let Err(e) = nats.write(container_name, object_name, data_bytes).await {
                        return Ok(Err(format!("failed to upload object: {e}")));
                    }
                }
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
        let plugin = CustomBlobstore::new();
        assert_eq!(plugin.id(), PLUGIN_ID);
    }

    #[test]
    fn test_world() {
        let plugin = CustomBlobstore::new();
        let world = plugin.world();

        assert!(
            world
                .exports
                .iter()
                .any(|i| i.namespace == "wasi" && i.package == "blobstore")
        );
    }

    #[test]
    fn test_default() {
        let plugin = CustomBlobstore::default();
        assert_eq!(plugin.id(), PLUGIN_ID);
    }
}
