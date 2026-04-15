# Multi-Backend KV Plugin Design

## Context

wasmCloud 有 6 个独立的 `wasi:keyvalue@0.2.0-draft` 实现（in-memory、redis、nats、filesystem 在 wash-runtime 内置，Cloudflare KV 在独立 crate），架构不统一，维护成本高。参照 `custom_plugin_blobstore` 用 OpenDAL 统一多后端的成功模式，将所有 KV 后端合并为一个多后端插件。

## Architecture

```
                    ┌─────────────────────────┐
                    │  wasi:keyvalue@0.2.0    │
                    │  (store, atomics, batch) │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   custom_plugin_kv       │
                    └────────────┬────────────┘
                                 │
               ┌─────────────────┼─────────────────┐
               │                 │                   │
     ┌─────────▼───┐   ┌────────▼──────┐   ┌──────▼──────┐
     │ OpenDAL     │   │ Cloudflare KV │   │ NATS KV     │
     │ (redis,     │   │ (SDK)         │   │ (JetStream) │
     │  memory, fs)│   │               │   │             │
     └─────────────┘   └───────────────┘   └─────────────┘
```

三引擎架构，通过 config 的 `backend` 字段选择：

- **OpenDAL**：`redis`、`memory`、`fs`
- **Cloudflare KV SDK**：`cloudflare`
- **NATS JetStream KV**：`nats`

## Config

```yaml
# Redis
wasi:keyvalue:
  config:
    backend: "redis"
    endpoint: "redis://127.0.0.1:6379"
    root: "/my-prefix/"

# Memory
wasi:keyvalue:
  config:
    backend: "memory"

# Filesystem
wasi:keyvalue:
  config:
    backend: "fs"
    root: "/data/kv/"

# Cloudflare KV
wasi:keyvalue:
  config:
    backend: "cloudflare"
    account_id: "xxx"
    api_token: "xxx"
    namespace_id: "xxx"

# NATS
wasi:keyvalue:
  config:
    backend: "nats"
    nats_url: "nats://127.0.0.1:4222"
    bucket: "my-kv-bucket"
```

## Key Design Decisions

### Bucket mapping (unified rule)

- `open("")` → fallback to config: `namespace_id` (cloudflare) / `root` (opendal) / `bucket` (nats)
- `open("xxx")` → use identifier as key prefix / namespace / bucket name

### KvEngine enum

```rust
enum KvEngine {
    OpenDal(Operator),
    Cloudflare { client: KvNamespaceClient },
    Nats { store: nats::kv::Bucket },
}
```

### ComponentData

```rust
struct ComponentData {
    interface_config: HashMap<String, String>,
    engine: Option<KvEngine>,
}
```

Engine lazy-created on first `open()` call, cached in ComponentData.

### BucketHandle

```rust
struct BucketHandle {
    engine: Arc<KvEngine>,
    identifier: String,
}
```

All operations dispatch through `BucketHandle.engine` based on engine type.

### Batch operations

- **OpenDAL**: sequential loop (for each key, call op.read/write/delete)
- **Cloudflare KV**: native `write_multiple` / `delete_multiple` SDK methods
- **NATS**: sequential loop

### Atomic increment

- **OpenDAL**: read-modify-write (same as current in-memory/fs implementation)
- **Cloudflare KV**: read-modify-write (existing behavior)
- **NATS**: native atomic increment via `Bucket::update` with version check

## Scope

### Rename

`custom_plugin_cf_kv` → `custom_plugin_kv` (crate name, directory, Cargo.toml)

### Delete from wash-runtime

Remove these files and their registrations:
- `crates/wash-runtime/src/plugin/wasi_keyvalue/in_memory.rs`
- `crates/wash-runtime/src/plugin/wasi_keyvalue/redis.rs`
- `crates/wash-runtime/src/plugin/wasi_keyvalue/nats.rs`
- `crates/wash-runtime/src/plugin/wasi_keyvalue/filesystem.rs`
- Corresponding mod.rs entries and feature flags

### New dependencies in custom_plugin_kv

- `opendal` (with `services-redis` feature)
- `async-nats` (for NATS JetStream KV)

### Plugin ID

Change from `"wasi-keyvalue-cf-kv"` to `"wasi-keyvalue-multi-backend"`

## Verification

1. `cargo build --workspace` passes
2. `cargo clippy --workspace` no warnings
3. `cargo test --workspace` passes
4. Build http-api-distributed example, verify KV operations work with memory backend
