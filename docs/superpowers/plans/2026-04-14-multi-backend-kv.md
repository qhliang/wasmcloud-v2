# Multi-Backend KV Plugin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Merge all 5 KV backend implementations into a single `custom_plugin_kv` crate with three-engine architecture (OpenDAL, Cloudflare SDK, NATS JetStream).

**Architecture:** Rename `custom_plugin_cf_kv` to `custom_plugin_kv`. Add OpenDAL engine for redis/memory/fs backends and NATS JetStream engine. Engine is selected via `backend` config field. Delete the 4 built-in KV implementations from `wash-runtime`. Remove `KeyValueBackendType` enum from `wash/src/cli/host.rs`.

**Tech Stack:** Rust, OpenDAL 0.53 (services-redis, services-memory, services-fs), async-nats 0.44, Cloudflare Workers KV SDK 0.1.2, wasmtime component bindings.

---

### Task 1: Rename crate `custom_plugin_cf_kv` → `custom_plugin_kv`

**Files:**
- Move: `crates/custom_plugin_cf_kv/` → `crates/custom_plugin_kv/`
- Modify: `crates/custom_plugin_kv/Cargo.toml`
- Modify: `Cargo.toml` (workspace members)
- Modify: `crates/wash/Cargo.toml` (dependency path)

- [ ] **Step 1: Rename the directory**

```bash
mv crates/custom_plugin_cf_kv crates/custom_plugin_kv
```

- [ ] **Step 2: Update `crates/custom_plugin_kv/Cargo.toml`**

Change package name and description:

```toml
name = "custom_plugin_kv"
description = "Multi-backend host plugin for wasi:keyvalue (OpenDAL, Cloudflare KV, NATS JetStream)"
```

- [ ] **Step 3: Update workspace `Cargo.toml`**

Replace `crates/custom_plugin_cf_kv` with `crates/custom_plugin_kv` in the members list.

- [ ] **Step 4: Update `crates/wash/Cargo.toml`**

Change dependency path:

```toml
custom_plugin_kv = { path = "../custom_plugin_kv" }
```

- [ ] **Step 5: Update import in `crates/wash/src/cli/host.rs`**

Change:
```rust
use custom_plugin_cf_kv::CloudflareKeyValue;
```
to:
```rust
use custom_plugin_kv::MultiBackendKeyValue;
```

- [ ] **Step 6: Verify build**

Run: `cargo build --workspace`
Expected: Compile errors in host.rs (expected — `MultiBackendKeyValue` doesn't exist yet)

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor: rename custom_plugin_cf_kv to custom_plugin_kv"
```

---

### Task 2: Add new dependencies to `custom_plugin_kv`

**Files:**
- Modify: `crates/custom_plugin_kv/Cargo.toml`

- [ ] **Step 1: Add opendal and async-nats dependencies**

Add to `[dependencies]` section after existing dependencies:

```toml
opendal = { version = "0.53", default-features = false, features = [
    "services-redis",
    "services-memory",
    "services-fs",
] }
async-nats = { workspace = true }
```

Keep existing `cloudflare-workers-kv-sdk-rs = "0.1.2"`.

- [ ] **Step 2: Verify dependencies resolve**

Run: `cargo check -p custom_plugin_kv`
Expected: Passes (no code changes yet)

- [ ] **Step 3: Commit**

```bash
git add crates/custom_plugin_kv/Cargo.toml
git commit -m "feat(kv): add opendal and async-nats dependencies"
```

---

### Task 3: Implement KvEngine enum and engine creation

**Files:**
- Modify: `crates/custom_plugin_kv/src/lib.rs`

This task adds the `KvEngine` enum, updates `ComponentData` to include an optional engine, and implements `get_or_create_engine()`.

- [ ] **Step 1: Add imports and KvEngine enum**

Add these imports at the top of `lib.rs` (alongside existing imports):

```rust
use std::str::FromStr;
use opendal::Operator;
use opendal::Scheme;
```

Add the `KvEngine` enum after the `PLUGIN_KEYVALUE_ID` const (rename the const too):

```rust
const PLUGIN_KEYVALUE_ID: &str = "wasi-keyvalue-multi-backend";
```

Add the enum:

```rust
/// Backend engine for KV operations
enum KvEngine {
    /// OpenDAL operator (redis, memory, fs)
    OpenDal(Operator),
    /// Cloudflare KV client
    Cloudflare { client: KvNamespaceClient },
    /// NATS JetStream KV store
    Nats { store: async_nats::jetstream::kv::Store },
}
```

- [ ] **Step 2: Update ComponentData**

Change the existing `ComponentData` to include an optional engine:

```rust
/// Per-component data.
struct ComponentData {
    /// Static interface config from wasmcloud config
    interface_config: HashMap<String, String>,
    /// Lazily-created backend engine
    engine: Option<KvEngine>,
}
```

- [ ] **Step 3: Implement `get_or_create_engine()` on the plugin struct**

Rename the plugin struct from `CloudflareKeyValue` to `MultiBackendKeyValue` and add the engine creation method:

```rust
/// Multi-backend keyvalue plugin
#[derive(Clone)]
pub struct MultiBackendKeyValue {
    /// Per-component state tracker
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
    /// Metrics
    metrics: Arc<KvMetrics>,
}
```

Rename metrics struct from `CloudflareKvMetrics` to `KvMetrics`. Update meter name:

```rust
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
```

Add `get_or_create_engine()` method:

```rust
impl MultiBackendKeyValue {
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
                        "No KV config found for component '{}'",
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
            "cloudflare" => {
                let account_id = interface_config.get("account_id").cloned().unwrap_or_default();
                let api_token = interface_config.get("api_token").cloned().unwrap_or_default();
                let namespace = interface_config.get("namespace_id").cloned().unwrap_or_default();
                let client = KvNamespaceClient::new(&account_id, &api_token, &namespace);
                KvEngine::Cloudflare { client }
            }
            "nats" => {
                let nats_url = interface_config.get("nats_url").cloned().unwrap_or_else(|| "nats://127.0.0.1:4222".to_string());
                let bucket = interface_config.get("bucket").cloned().unwrap_or_else(|| "default".to_string());
                let client = async_nats::connect(&nats_url).await
                    .map_err(|e| anyhow::anyhow!("failed to connect to NATS: {e}"))?;
                let jetstream = async_nats::jetstream::new(client);
                let store = jetstream.get_key_value(&bucket).await
                    .map_err(|e| anyhow::anyhow!("failed to get NATS KV bucket '{bucket}': {e}"))?;
                KvEngine::Nats { store }
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
                    anyhow::anyhow!("failed to create OpenDAL operator for backend '{backend}': {e}")
                })?;
                KvEngine::OpenDal(op)
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
```

Add `clone_engine()` helper to `KvEngine`:

```rust
impl KvEngine {
    fn clone_engine(&self) -> KvEngine {
        match self {
            KvEngine::OpenDal(op) => KvEngine::OpenDal(op.clone()),
            KvEngine::Cloudflare { client } => KvEngine::Cloudflare { client: client.clone() },
            KvEngine::Nats { store } => KvEngine::Nats { store: store.clone() },
        }
    }
}
```

- [ ] **Step 4: Update `on_workload_item_bind` to store engine as None initially**

Change the `add_component` call to include `engine: None`:

```rust
self.tracker.write().await.add_component(
    component_handle,
    ComponentData {
        interface_config,
        engine: None,
    },
);
```

- [ ] **Step 5: Update `on_workload_unbind` log message**

Change `"CloudflareKeyValue plugin unbound"` to `"KV plugin unbound"`.

- [ ] **Step 6: Verify build**

Run: `cargo check -p custom_plugin_kv`
Expected: Compile errors in the trait impl blocks (expected — `BucketHandle` and `CloudflareKeyValue` references need updating, handled in next task)

- [ ] **Step 7: Commit**

```bash
git add crates/custom_plugin_kv/src/lib.rs
git commit -m "feat(kv): add KvEngine enum with OpenDAL/Cloudflare/NATS backends"
```

---

### Task 4: Rewrite BucketHandle and all interface implementations

**Files:**
- Modify: `crates/custom_plugin_kv/src/lib.rs`

This is the largest task — rewrite `BucketHandle` to hold an `Arc<KvEngine>` and dispatch all operations by engine type.

- [ ] **Step 1: Rewrite BucketHandle**

Replace the existing `BucketHandle` with:

```rust
/// Resource handle for a KV bucket
#[derive(Clone)]
pub struct BucketHandle {
    /// Backend engine
    engine: Arc<KvEngine>,
    /// Bucket/namespace identifier
    identifier: String,
}
```

- [ ] **Step 2: Rewrite `store::Host::open()`**

Replace the existing `open()` implementation:

```rust
impl<'a> bindings::wasi::keyvalue::store::Host for ActiveCtx<'a> {
    async fn open(
        &mut self,
        identifier: String,
    ) -> wasmtime::Result<Result<Resource<BucketHandle>, StoreError>> {
        let Some(plugin) = self.get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other(
                "KV keyvalue plugin not available".to_string(),
            )));
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
                    let backend = data.interface_config.get("backend").cloned().unwrap_or_default();
                    match backend.as_str() {
                        "cloudflare" => data.interface_config.get("namespace_id").cloned().unwrap_or_default(),
                        "nats" => data.interface_config.get("bucket").cloned().unwrap_or_default(),
                        _ => data.interface_config.get("root").cloned().unwrap_or_default(),
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
```

- [ ] **Step 3: Rewrite `store::HostBucket` (get/set/delete/exists/list_keys/drop)**

Replace the entire `HostBucket` impl block. Each method dispatches based on engine type:

```rust
impl<'a> bindings::wasi::keyvalue::store::HostBucket for ActiveCtx<'a> {
    async fn get(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
    ) -> wasmtime::Result<Result<Option<Vec<u8>>, StoreError>> {
        let Some(plugin) = self.get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other("KV plugin not available".to_string())));
        };
        plugin.record_operation("get");

        let bucket_handle = self.table.get(&bucket)?;
        let full_key = format!("{}/{}", bucket_handle.identifier, key);

        match bucket_handle.engine.as_ref() {
            KvEngine::OpenDal(op) => {
                match op.read(&full_key).await {
                    Ok(data) => Ok(Ok(Some(data.to_vec()))),
                    Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(Ok(None)),
                    Err(e) => Ok(Err(StoreError::Other(format!("{e}")))),
                }
            }
            KvEngine::Cloudflare { client } => {
                let client = client.clone();
                let result = cf_result_to_string(client.get(&key).await);
                match result {
                    Ok(value) if !value.is_empty() => {
                        if let Ok(decoded) = base64::Engine::decode(
                            &base64::engine::general_purpose::STANDARD, &value,
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
                let nats_key = if bucket_handle.identifier.is_empty() { key.clone() } else { format!("{}.{}", bucket_handle.identifier, key) };
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
        let Some(plugin) = self.get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other("KV plugin not available".to_string())));
        };
        plugin.record_operation("set");

        let bucket_handle = self.table.get(&bucket)?;
        let full_key = format!("{}/{}", bucket_handle.identifier, key);

        match bucket_handle.engine.as_ref() {
            KvEngine::OpenDal(op) => {
                match op.write(&full_key, value).await {
                    Ok(_) => Ok(Ok(())),
                    Err(e) => Ok(Err(StoreError::Other(format!("{e}")))),
                }
            }
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
                let nats_key = if bucket_handle.identifier.is_empty() { key.clone() } else { format!("{}.{}", bucket_handle.identifier, key) };
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
        let Some(plugin) = self.get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other("KV plugin not available".to_string())));
        };
        plugin.record_operation("delete");

        let bucket_handle = self.table.get(&bucket)?;
        let full_key = format!("{}/{}", bucket_handle.identifier, key);

        match bucket_handle.engine.as_ref() {
            KvEngine::OpenDal(op) => {
                match op.delete(&full_key).await {
                    Ok(_) => Ok(Ok(())),
                    Err(e) => Ok(Err(StoreError::Other(format!("{e}")))),
                }
            }
            KvEngine::Cloudflare { client } => {
                match cf_result_to_string(client.delete(&key).await) {
                    Ok(_) => Ok(Ok(())),
                    Err(e) => Ok(Err(StoreError::Other(e))),
                }
            }
            KvEngine::Nats { store } => {
                let nats_key = if bucket_handle.identifier.is_empty() { key.clone() } else { format!("{}.{}", bucket_handle.identifier, key) };
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
        let Some(plugin) = self.get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other("KV plugin not available".to_string())));
        };
        plugin.record_operation("exists");

        let bucket_handle = self.table.get(&bucket)?;
        let full_key = format!("{}/{}", bucket_handle.identifier, key);

        match bucket_handle.engine.as_ref() {
            KvEngine::OpenDal(op) => {
                match op.exists(&full_key).await {
                    Ok(exists) => Ok(Ok(exists)),
                    Err(_) => Ok(Ok(false)),
                }
            }
            KvEngine::Cloudflare { client } => {
                match cf_result_to_string(client.read_metadata(&key).await) {
                    Ok(_) => Ok(Ok(true)),
                    Err(_) => Ok(Ok(false)),
                }
            }
            KvEngine::Nats { store } => {
                let nats_key = if bucket_handle.identifier.is_empty() { key.clone() } else { format!("{}.{}", bucket_handle.identifier, key) };
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
        let Some(plugin) = self.get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other("KV plugin not available".to_string())));
        };
        plugin.record_operation("list_keys");

        let bucket_handle = self.table.get(&bucket)?;
        let prefix = &bucket_handle.identifier;

        match bucket_handle.engine.as_ref() {
            KvEngine::OpenDal(op) => {
                let path = if prefix.is_empty() { "/".to_string() } else { format!("{prefix}/") };
                match op.list(&path).await {
                    Ok(entries) => {
                        let keys: Vec<String> = entries.into_iter()
                            .map(|e| e.name().to_string())
                            .collect();
                        let start_index = cursor.unwrap_or(0) as usize;
                        const PAGE_SIZE: usize = 100;
                        let end_index = std::cmp::min(start_index + PAGE_SIZE, keys.len());
                        let page_keys = keys.get(start_index..end_index).unwrap_or_default().to_vec();
                        let next_cursor = if end_index < keys.len() { Some(end_index as u64) } else { None };
                        Ok(Ok(KeyResponse { keys: page_keys, cursor: next_cursor }))
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
                        let page_keys = all_keys.get(start_index..end_index).unwrap_or_default().to_vec();
                        let next_cursor = if end_index < all_keys.len() { Some(end_index as u64) } else { None };
                        Ok(Ok(KeyResponse { keys: page_keys, cursor: next_cursor }))
                    }
                    Err(e) => Ok(Err(StoreError::Other(e))),
                }
            }
            KvEngine::Nats { store } => {
                let keys_iter = match store.keys().await {
                    Ok(i) => i,
                    Err(e) => return Ok(Err(StoreError::Other(format!("{e}")))),
                };
                let prefix_filter = if prefix.is_empty() { "".to_string() } else { format!("{prefix}.") };
                let all_keys: Vec<String> = keys_iter
                    .filter_map(|r| r.ok())
                    .filter(|k| prefix_filter.is_empty() || k.starts_with(&prefix_filter))
                    .map(|k| if prefix_filter.is_empty() { k } else { k.strip_prefix(&prefix_filter).unwrap_or(&k).to_string() })
                    .collect();
                let start_index = cursor.unwrap_or(0) as usize;
                const PAGE_SIZE: usize = 100;
                let end_index = std::cmp::min(start_index + PAGE_SIZE, all_keys.len());
                let page_keys = all_keys.get(start_index..end_index).unwrap_or_default().to_vec();
                let next_cursor = if end_index < all_keys.len() { Some(end_index as u64) } else { None };
                Ok(Ok(KeyResponse { keys: page_keys, cursor: next_cursor }))
            }
        }
    }

    async fn drop(&mut self, rep: Resource<BucketHandle>) -> wasmtime::Result<()> {
        debug!(resource_id = ?rep, "Dropping bucket resource");
        self.table.delete(rep)?;
        Ok(())
    }
}
```

- [ ] **Step 4: Rewrite `atomics::Host::increment()`**

```rust
impl<'a> bindings::wasi::keyvalue::atomics::Host for ActiveCtx<'a> {
    async fn increment(
        &mut self,
        bucket: Resource<BucketHandle>,
        key: String,
        delta: u64,
    ) -> wasmtime::Result<Result<u64, StoreError>> {
        let Some(plugin) = self.get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other("KV plugin not available".to_string())));
        };
        plugin.record_operation("increment");

        let bucket_handle = self.table.get(&bucket)?;
        let full_key = format!("{}/{}", bucket_handle.identifier, key);

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
                        let bytes: Bytes = if let Ok(decoded) =
                            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &value)
                        {
                            Bytes::from(decoded)
                        } else {
                            Bytes::from(value.into_bytes())
                        };
                        if bytes.len() == 8 { bytes.get_u64() } else { String::from_utf8_lossy(&bytes).parse::<u64>().unwrap_or(0) }
                    }
                    _ => 0,
                };
                let new_value = current_value.saturating_add(delta);
                let value_bytes = new_value.to_be_bytes().to_vec();
                let value_str = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &value_bytes);
                let kv_request = KvRequest::new(&key, &value_str);
                match cf_result_to_string(client.write(kv_request).await) {
                    Ok(_) => Ok(Ok(new_value)),
                    Err(e) => Ok(Err(StoreError::Other(e))),
                }
            }
            KvEngine::Nats { store } => {
                // CAS via revision check
                let nats_key = if bucket_handle.identifier.is_empty() { key.clone() } else { format!("{}.{}", bucket_handle.identifier, key) };
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
```

- [ ] **Step 5: Rewrite `batch::Host` (get_many/set_many/delete_many)**

```rust
impl<'a> bindings::wasi::keyvalue::batch::Host for ActiveCtx<'a> {
    async fn get_many(
        &mut self,
        bucket: Resource<BucketHandle>,
        keys: Vec<String>,
    ) -> wasmtime::Result<Result<Vec<Option<(String, Vec<u8>)>>, StoreError>> {
        let Some(plugin) = self.get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other("KV plugin not available".to_string())));
        };
        plugin.record_operation("get_many");

        let bucket_handle = self.table.get(&bucket)?;
        let prefix = &bucket_handle.identifier;

        match bucket_handle.engine.as_ref() {
            KvEngine::OpenDal(op) => {
                let mut results = Vec::with_capacity(keys.len());
                for key in keys {
                    let full_key = format!("{prefix}/{key}");
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
                                &base64::engine::general_purpose::STANDARD, &value,
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
                    let nats_key = if prefix.is_empty() { key.clone() } else { format!("{prefix}.{key}") };
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
        let Some(plugin) = self.get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other("KV plugin not available".to_string())));
        };
        plugin.record_operation("set_many");

        let bucket_handle = self.table.get(&bucket)?;
        let prefix = &bucket_handle.identifier;

        match bucket_handle.engine.as_ref() {
            KvEngine::OpenDal(op) => {
                for (key, value) in key_values {
                    let full_key = format!("{prefix}/{key}");
                    if let Err(e) = op.write(&full_key, value).await {
                        return Ok(Err(StoreError::Other(format!("{e}"))));
                    }
                }
                Ok(Ok(()))
            }
            KvEngine::Cloudflare { client } => {
                let kv_requests: Vec<KvRequest> = key_values.into_iter().map(|(key, value)| {
                    let should_base64 = !value.iter().all(|b| b.is_ascii() && *b >= 32);
                    let value_str = if should_base64 {
                        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &value)
                    } else {
                        String::from_utf8_lossy(&value).to_string()
                    };
                    KvRequest::new(&key, &value_str)
                }).collect();
                match cf_result_to_string(client.write_multiple(kv_requests).await) {
                    Ok(_) => Ok(Ok(())),
                    Err(e) => Ok(Err(StoreError::Other(e))),
                }
            }
            KvEngine::Nats { store } => {
                for (key, value) in key_values {
                    let nats_key = if prefix.is_empty() { key.clone() } else { format!("{prefix}.{key}") };
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
        let Some(plugin) = self.get_plugin::<MultiBackendKeyValue>(PLUGIN_KEYVALUE_ID) else {
            return Ok(Err(StoreError::Other("KV plugin not available".to_string())));
        };
        plugin.record_operation("delete_many");

        let bucket_handle = self.table.get(&bucket)?;
        let prefix = &bucket_handle.identifier;

        match bucket_handle.engine.as_ref() {
            KvEngine::OpenDal(op) => {
                for key in keys {
                    let full_key = format!("{prefix}/{key}");
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
                    let nats_key = if prefix.is_empty() { key.clone() } else { format!("{prefix}.{key}") };
                    if let Err(e) = store.delete(&nats_key).await {
                        return Ok(Err(StoreError::Other(format!("{e}"))));
                    }
                }
                Ok(Ok(()))
            }
        }
    }
}
```

- [ ] **Step 6: Update `HostPlugin` impl**

Rename `CloudflareKeyValue` to `MultiBackendKeyValue` in the `HostPlugin` impl block. Update log messages from `"CloudflareKeyValue"` to `"KV"`.

- [ ] **Step 7: Update `Default` impl**

```rust
impl Default for MultiBackendKeyValue {
    fn default() -> Self {
        Self::new()
    }
}
```

- [ ] **Step 8: Update tests**

```rust
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
            world.exports.iter().any(|i| i.namespace == "wasi" && i.package == "keyvalue")
        );
    }

    #[test]
    fn test_default() {
        let plugin = MultiBackendKeyValue::default();
        assert_eq!(plugin.id(), PLUGIN_KEYVALUE_ID);
    }
}
```

- [ ] **Step 9: Remove unused imports**

After the full rewrite, the following imports are no longer needed directly (they are used through KvEngine):
- Remove `use bytes::{Buf, Bytes};` if no longer used directly (Bytes is used in NATS increment — keep it)

Verify: `cargo check -p custom_plugin_kv`
Expected: PASS

- [ ] **Step 10: Commit**

```bash
git add crates/custom_plugin_kv/src/lib.rs
git commit -m "feat(kv): implement multi-backend KV with OpenDAL/Cloudflare/NATS engines"
```

---

### Task 5: Update host.rs to use MultiBackendKeyValue

**Files:**
- Modify: `crates/wash/src/cli/host.rs`

- [ ] **Step 1: Remove KeyValueBackendType enum and backend selection logic**

Replace the `KeyValueBackendType` enum and `keyvalue_backend` field entirely. Remove the `match self.keyvalue_backend` block. Instead, always register `MultiBackendKeyValue`:

```rust
use custom_plugin_kv::MultiBackendKeyValue;
```

Remove:
```rust
#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub enum KeyValueBackendType {
    #[default]
    Nats,
    Cloudflare,
}
```

Remove:
```rust
/// The keyvalue backend to use
#[clap(long = "keyvalue-backend", env = "KEYVALUE_BACKEND")]
pub keyvalue_backend: KeyValueBackendType,
```

Replace the match block with:

```rust
// Enable multi-backend KV plugin
cluster_host_builder = cluster_host_builder
    .with_plugin(Arc::new(MultiBackendKeyValue::new()))?;
tracing::info!("Multi-backend KV plugin enabled");
```

- [ ] **Step 2: Verify build**

Run: `cargo build --workspace`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add crates/wash/src/cli/host.rs
git commit -m "refactor: use MultiBackendKeyValue, remove KeyValueBackendType"
```

---

### Task 6: Delete built-in KV implementations from wash-runtime

**Files:**
- Delete: `crates/wash-runtime/src/plugin/wasi_keyvalue/in_memory.rs`
- Delete: `crates/wash-runtime/src/plugin/wasi_keyvalue/redis.rs`
- Delete: `crates/wash-runtime/src/plugin/wasi_keyvalue/nats.rs`
- Delete: `crates/wash-runtime/src/plugin/wasi_keyvalue/filesystem.rs`
- Modify: `crates/wash-runtime/src/plugin/wasi_keyvalue/mod.rs`
- Modify: `crates/wash-runtime/src/plugin/mod.rs`
- Modify: `crates/wash-runtime/Cargo.toml` (remove `redis` dep if unused elsewhere)

- [ ] **Step 1: Delete the four backend files**

```bash
rm crates/wash-runtime/src/plugin/wasi_keyvalue/in_memory.rs
rm crates/wash-runtime/src/plugin/wasi_keyvalue/redis.rs
rm crates/wash-runtime/src/plugin/wasi_keyvalue/nats.rs
rm crates/wash-runtime/src/plugin/wasi_keyvalue/filesystem.rs
```

- [ ] **Step 2: Clear `mod.rs`**

Replace `crates/wash-runtime/src/plugin/wasi_keyvalue/mod.rs` with an empty module:

```rust
// Built-in KV implementations have been moved to custom_plugin_kv (multi-backend).
// This module is kept as a placeholder for the wasi-keyvalue feature flag.
```

- [ ] **Step 3: Check if `redis` crate is used elsewhere in wash-runtime**

Run: `grep -r "redis" crates/wash-runtime/src/ --include="*.rs"`
If no remaining usage, remove `redis = { workspace = true }` from `crates/wash-runtime/Cargo.toml`.

- [ ] **Step 4: Check if `futures` crate is still needed**

Run: `grep -r "futures" crates/wash-runtime/src/ --include="*.rs"`
If no remaining usage, remove `futures = { workspace = true }` from `crates/wash-runtime/Cargo.toml`.

- [ ] **Step 5: Verify build**

Run: `cargo build --workspace`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: remove built-in KV backends from wash-runtime"
```

---

### Task 7: Clippy and fmt

**Files:**
- May modify: `crates/custom_plugin_kv/src/lib.rs` (clippy fixes)

- [ ] **Step 1: Run clippy**

Run: `cargo clippy --workspace 2>&1 | head -50`
Fix any warnings. Common issues:
- Unused imports
- Dead code
- Type complexity in batch methods

- [ ] **Step 2: Run fmt**

Run: `cargo +nightly fmt`

- [ ] **Step 3: Verify clean**

Run: `cargo clippy --workspace`
Expected: No warnings

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "style: fix clippy warnings and apply fmt"
```

---

### Task 8: Run tests

- [ ] **Step 1: Run workspace tests**

Run: `cargo test --workspace`
Expected: All tests pass

- [ ] **Step 2: Final commit if any test fixes needed**

```bash
git add -A
git commit -m "fix: resolve test failures"
```
