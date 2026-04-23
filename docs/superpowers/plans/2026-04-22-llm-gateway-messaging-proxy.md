# llm-gateway-messaging-proxy 实现计划

## Task 1: 创建 scaffolding

**文件**: `crates/llm-gateway-messaging-proxy/`（新建）

- `Cargo.toml` — wit-bindgen 0.46.0, serde, serde_json, edition 2024, cdylib
- `wit/world.wit` — 导入 wasi:logging, wasmcloud:messaging/consumer, wasi:config/store; 导出 custom:llm-gateway/chat
- `wit/deps/` — 从现有位置复制
- `src/lib.rs` — 实现 chat() 函数

## Task 2: 实现 chat 逻辑

- `chat()` 被调用时：
  1. 从 `wasi:config/store::get()` 读取 `llm_request_subject`（默认 `"llm-gateway.chat"`）和 `llm_request_timeout_ms`（默认 `"30000"`）
  2. 将参数序列化为 JSON（与 messaging 组件请求格式一致）
  3. 调用 `consumer::request(subject, body, timeout_ms)`
  4. 解析响应 JSON → `ChatResponse`，或错误 JSON → `LlmError`

## Task 3: 验证

- `cargo build --target wasm32-wasip2 --release`
- clippy, fmt
- commit
